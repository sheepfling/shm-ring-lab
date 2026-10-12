#include "shm_ring_lab/session.hpp"

#include "platform/backend.hpp"
#include "protocol.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::chrono_literals;
using shm_ring_lab::Session;
using shm_ring_lab::Status;

namespace {

void check(Status status, const char* operation) {
    if (status != Status::ok) {
        throw std::runtime_error(std::string(operation) + ": " + shm_ring_lab::to_string(status));
    }
}

std::string message_text(const shm_ring_lab::Message& message) {
    return std::string(message.begin(), message.end());
}

void write_ready(const std::string& path) {
    std::ofstream marker(path, std::ios::binary | std::ios::trunc);
    marker << "ready\n";
    marker.flush();
    if (!marker) throw std::runtime_error("failed to write readiness marker");
}

int produce(const std::string& name, std::uint32_t count, const std::string& prefix) {
    auto session = Session::open(name);
    check(session.claim_writer(), "claim_writer");
    for (std::uint32_t i = 0; i < count; ++i) {
        const auto text = prefix + std::to_string(i);
        const auto result = session.send(text.data(), text.size(), 5s);
        check(result, "send");
    }
    check(session.close_writer(), "close_writer");
    return 0;
}

int consume(const std::string& name, std::uint32_t count, const std::string& prefix) {
    auto session = Session::open(name);
    check(session.claim_reader(), "claim_reader");
    for (std::uint32_t i = 0; i < count; ++i) {
        auto result = session.receive(5s);
        check(result.status, "receive");
        if (message_text(result.message) != prefix + std::to_string(i)) {
            throw std::runtime_error("message ordering/content mismatch at index " + std::to_string(i));
        }
    }
    const auto closed = session.receive(2s);
    if (closed.status != Status::closed) {
        throw std::runtime_error("reader did not observe clean end-of-stream");
    }
    return 0;
}

int duplex(const std::string& outgoing_name, const std::string& incoming_name,
           const std::string& own_prefix, const std::string& peer_prefix,
           std::uint32_t count) {
    auto outgoing = Session::open(outgoing_name);
    auto incoming = Session::open(incoming_name);
    check(outgoing.claim_writer(), "claim outgoing writer");
    check(incoming.claim_reader(), "claim incoming reader");
    std::string reader_error;
    std::thread receiver([&] {
        try {
            for (std::uint32_t i = 0; i < count; ++i) {
                auto result = incoming.receive(8s);
                check(result.status, "duplex receive");
                if (message_text(result.message) != peer_prefix + std::to_string(i)) {
                    throw std::runtime_error("duplex ordering/content mismatch");
                }
            }
        } catch (const std::exception& error) {
            reader_error = error.what();
        }
    });
    try {
        for (std::uint32_t i = 0; i < count; ++i) {
            const auto text = own_prefix + std::to_string(i);
            check(outgoing.send(text.data(), text.size(), 8s), "duplex send");
        }
        check(outgoing.close_writer(), "duplex close writer");
    } catch (...) {
        receiver.join();
        throw;
    }
    receiver.join();
    if (!reader_error.empty()) throw std::runtime_error(reader_error);
    return 0;
}

int claim_and_wait(const std::string& name, const std::string& ready_path) {
    auto session = Session::open(name);
    check(session.claim_reader(), "claim_reader");
    write_ready(ready_path);
    std::this_thread::sleep_for(30s);
    return 0;
}

int wait_on_empty_then_receive(const std::string& name, const std::string& ready_path) {
    auto session = Session::open(name);
    check(session.claim_reader(), "claim_reader");
    const auto empty = session.receive(100ms);
    if (empty.status != Status::timeout) {
        throw std::runtime_error("empty receive did not time out");
    }
    write_ready(ready_path);
    const auto message = session.receive(5s);
    check(message.status, "receive after empty timeout");
    if (message_text(message.message) != "wake0") {
        throw std::runtime_error("wrong message after empty timeout");
    }
    if (session.receive(2s).status != Status::closed) {
        throw std::runtime_error("writer close was not observed after empty timeout");
    }
    return 0;
}

int crash_while_holding_lock(const std::string& name, const std::string& ready_path) {
    auto mapping = shm_ring_lab::detail::platform::open(name);
    if (shm_ring_lab::detail::platform::lock_for(mapping, 5s) !=
        shm_ring_lab::detail::platform::LockResult::acquired) {
        throw std::runtime_error("test child could not acquire the platform lock");
    }
    write_ready(ready_path);
    // Deliberately bypass cleanup to exercise the documented dead-owner policy.
    std::_Exit(0);
}

int corrupt_frame(const std::string& name) {
    auto mapping = shm_ring_lab::detail::platform::open(name);
    if (shm_ring_lab::detail::platform::lock_for(mapping, 2s) !=
        shm_ring_lab::detail::platform::LockResult::acquired) {
        throw std::runtime_error("test child could not acquire the platform lock");
    }
    auto* header = static_cast<shm_ring_lab::detail::SharedHeader*>(mapping.address);
    header->read_index = 0;
    header->write_index = 1;
    header->count = 1;
    const auto length = header->max_payload_bytes + 1U;
    auto* slot = static_cast<std::uint8_t*>(mapping.address) +
        shm_ring_lab::detail::slot_offset(0, header->max_payload_bytes);
    std::memcpy(slot, &length, sizeof(length));
    shm_ring_lab::detail::platform::unlock(mapping);
    shm_ring_lab::detail::platform::close(mapping);
    return 0;
}

int corrupt_version(const std::string& name) {
    auto mapping = shm_ring_lab::detail::platform::open(name);
    if (shm_ring_lab::detail::platform::lock_for(mapping, 2s) !=
        shm_ring_lab::detail::platform::LockResult::acquired) {
        throw std::runtime_error("test child could not acquire the platform lock");
    }
    auto* header = static_cast<shm_ring_lab::detail::SharedHeader*>(mapping.address);
    header->protocol_version = shm_ring_lab::detail::kProtocolVersion + 1U;
    shm_ring_lab::detail::platform::unlock(mapping);
    shm_ring_lab::detail::platform::close(mapping);
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 5 && std::string(argv[1]) == "produce") {
            return produce(argv[2], static_cast<std::uint32_t>(std::stoul(argv[3])), argv[4]);
        }
        if (argc == 5 && std::string(argv[1]) == "consume") {
            return consume(argv[2], static_cast<std::uint32_t>(std::stoul(argv[3])), argv[4]);
        }
        if (argc == 7 && std::string(argv[1]) == "duplex") {
            return duplex(argv[2], argv[3], argv[4], argv[5], static_cast<std::uint32_t>(std::stoul(argv[6])));
        }
        if (argc == 4 && std::string(argv[1]) == "claim-wait") return claim_and_wait(argv[2], argv[3]);
        if (argc == 4 && std::string(argv[1]) == "empty-wait") return wait_on_empty_then_receive(argv[2], argv[3]);
        if (argc == 4 && std::string(argv[1]) == "crash-lock") return crash_while_holding_lock(argv[2], argv[3]);
        if (argc == 3 && std::string(argv[1]) == "corrupt-frame") return corrupt_frame(argv[2]);
        if (argc == 3 && std::string(argv[1]) == "corrupt-version") return corrupt_version(argv[2]);
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "test peer: " << error.what() << '\n';
        return 1;
    }
}
