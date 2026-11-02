#include "shm_ring_lab/session.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::chrono_literals;
using shm_ring_lab::Session;
using shm_ring_lab::Status;

namespace {

void require_ok(Status status, const char* action) {
    if (status != Status::ok) {
        throw std::runtime_error(std::string(action) + ": " + shm_ring_lab::to_string(status));
    }
}

void usage() {
    std::cerr <<
        "Usage:\n"
        "  shm_ring_demo create <name> [capacity] [max-payload-bytes]\n"
        "  shm_ring_demo host <name> [capacity] [max-payload-bytes]\n"
        "  shm_ring_demo produce <name>\n"
        "  shm_ring_demo consume <name>\n"
        "  shm_ring_demo duplex <outgoing-name> <incoming-name> [ready-file]\n"
        "  shm_ring_demo cleanup <name>\n"
        "Producer reads stdin lines and closes its writer at EOF. Consumer drains\n"
        "messages and exits after the writer closes. Duplex runs one reader thread\n"
        "and one stdin writer over two independent directional sessions. Host keeps\n"
        "the created session open until Enter, which is required for named objects\n"
        "on Windows.\n";
}

std::uint32_t parse_u32(const char* text, const char* label) {
    const auto value = std::stoul(text);
    if (value > 0xffffffffUL) throw std::invalid_argument(std::string(label) + " is too large");
    return static_cast<std::uint32_t>(value);
}

int produce(const std::string& name) {
    auto session = Session::open(name);
    require_ok(session.claim_writer(), "claim writer");
    std::string line;
    while (std::getline(std::cin, line)) {
        const auto status = session.send(line.data(), line.size(), 5s);
        require_ok(status, "send");
    }
    require_ok(session.close_writer(), "close writer");
    return 0;
}

int consume(const std::string& name) {
    auto session = Session::open(name);
    require_ok(session.claim_reader(), "claim reader");
    for (;;) {
        auto result = session.receive(5s);
        if (result.status == Status::closed) return 0;
        if (result.status == Status::timeout) {
            std::cerr << "No message before the 5-second receive deadline; retrying.\n";
            continue;
        }
        require_ok(result.status, "receive");
        std::cout.write(reinterpret_cast<const char*>(result.message.data()),
                        static_cast<std::streamsize>(result.message.size()));
        std::cout << '\n' << std::flush;
    }
}

struct InputState {
    std::mutex mutex;
    std::condition_variable ready;
    std::deque<std::string> lines;
    bool eof = false;
    bool stop = false;
};

int duplex(const std::string& outgoing_name, const std::string& incoming_name,
           const std::string& ready_file = {}) {
    auto outgoing = Session::open(outgoing_name);
    auto incoming = Session::open(incoming_name);
    require_ok(outgoing.claim_writer(), "claim outgoing writer");
    require_ok(incoming.claim_reader(), "claim incoming reader");

    std::atomic<bool> reader_failed{false};
    std::atomic<bool> stop_reader{false};
    auto input = std::make_shared<InputState>();
    std::thread input_reader([input] {
        std::string line;
        while (std::getline(std::cin, line)) {
            std::unique_lock<std::mutex> lock(input->mutex);
            input->ready.wait(lock, [&] { return input->lines.size() < 64 || input->stop; });
            if (input->stop) break;
            input->lines.push_back(std::move(line));
            lock.unlock();
            input->ready.notify_one();
        }
        {
            std::lock_guard<std::mutex> guard(input->mutex);
            input->eof = true;
        }
        input->ready.notify_all();
    });
    std::mutex output_mutex;
    std::thread reader([&, input] {
        for (;;) {
            if (stop_reader.load()) return;
            auto result = incoming.receive(100ms);
            if (result.status == Status::timeout) {
                continue;
            }
            if (result.status == Status::closed) return;
            if (result.status != Status::ok) {
                reader_failed.store(true);
                {
                    std::lock_guard<std::mutex> guard(input->mutex);
                    input->stop = true;
                }
                input->ready.notify_all();
                return;
            }
            std::lock_guard<std::mutex> guard(output_mutex);
            std::cout << "peer: ";
            std::cout.write(reinterpret_cast<const char*>(result.message.data()),
                            static_cast<std::streamsize>(result.message.size()));
            std::cout << '\n' << std::flush;
        }
    });

    if (!ready_file.empty()) {
        std::ofstream ready(ready_file, std::ios::binary | std::ios::trunc);
        ready << "ready\n";
        ready.flush();
        if (!ready) {
            reader_failed.store(true);
            {
                std::lock_guard<std::mutex> guard(input->mutex);
                input->stop = true;
            }
            input->ready.notify_all();
        }
    }

    bool input_eof = false;
    while (!reader_failed.load()) {
        std::string line;
        {
            std::unique_lock<std::mutex> lock(input->mutex);
            input->ready.wait_for(lock, 50ms, [&] {
                return !input->lines.empty() || input->eof || reader_failed.load();
            });
            if (reader_failed.load()) break;
            if (input->lines.empty()) {
                if (input->eof) {
                    input_eof = true;
                    break;
                }
                continue;
            }
            line = std::move(input->lines.front());
            input->lines.pop_front();
            input->ready.notify_one();
        }
        if (outgoing.send(line.data(), line.size(), 5s) != Status::ok) {
            reader_failed.store(true);
            {
                std::lock_guard<std::mutex> guard(input->mutex);
                input->stop = true;
            }
            input->ready.notify_all();
        }
    }

    const auto close_status = outgoing.close_writer();
    stop_reader.store(true);
    if (!input_eof) {
        std::lock_guard<std::mutex> guard(input->mutex);
        input->stop = true;
    }
    input->ready.notify_all();
    reader.join();
    if (input_eof) {
        input_reader.join();
    } else {
        // C++ has no portable way to cancel a thread blocked in getline. On a
        // fatal duplex error, let process exit close stdin and its OS handles.
        input_reader.detach();
    }
    if (close_status != Status::ok || reader_failed.load()) {
        std::cerr << "duplex receive/send failed; outgoing writer closed\n" << std::flush;
        if (!input_eof) std::_Exit(1);
        throw std::runtime_error("duplex receive/send failed");
    }
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc >= 3 && std::string(argv[1]) == "host") {
            shm_ring_lab::Options options;
            if (argc > 3) options.capacity = parse_u32(argv[3], "capacity");
            if (argc > 4) options.max_payload_bytes = parse_u32(argv[4], "max-payload-bytes");
            if (argc > 5) { usage(); return 2; }
            [[maybe_unused]] auto session = Session::create(argv[2], options);
            std::cout << "Hosting '" << argv[2] << "'. Peers may connect; press Enter "
                      << "after all peers exit to release the session handles.\n" << std::flush;
            std::string line;
            std::getline(std::cin, line);
            return 0;
        }
        if (argc >= 3 && std::string(argv[1]) == "create") {
            shm_ring_lab::Options options;
            if (argc > 3) options.capacity = parse_u32(argv[3], "capacity");
            if (argc > 4) options.max_payload_bytes = parse_u32(argv[4], "max-payload-bytes");
            if (argc > 5) { usage(); return 2; }
            auto session = Session::create(argv[2], options);
            std::cout << "Created '" << argv[2] << "' with capacity " << options.capacity
                      << " and max payload " << options.max_payload_bytes << " bytes.\n";
            return 0;
        }
        if (argc == 3 && std::string(argv[1]) == "produce") return produce(argv[2]);
        if (argc == 3 && std::string(argv[1]) == "consume") return consume(argv[2]);
        if ((argc == 4 || argc == 5) && std::string(argv[1]) == "duplex") {
            return duplex(argv[2], argv[3], argc == 5 ? argv[4] : "");
        }
        if (argc == 3 && std::string(argv[1]) == "cleanup") {
            if (!Session::remove(argv[2])) throw std::runtime_error("session cleanup failed");
            return 0;
        }
        usage();
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "shm_ring_demo: " << error.what() << '\n';
        return 1;
    }
}
