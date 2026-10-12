#include "shm_ring_lab/session.hpp"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace std::chrono_literals;

namespace {

void check(shm_ring_lab::Status status, const char* action) {
    if (status != shm_ring_lab::Status::ok) {
        throw std::runtime_error(std::string(action) + ": " + shm_ring_lab::to_string(status));
    }
}

std::uint32_t parse(const char* text) {
    const auto value = std::stoul(text);
    if (value > 0xffffffffUL) throw std::invalid_argument("numeric argument too large");
    return static_cast<std::uint32_t>(value);
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 5 || (std::string(argv[1]) != "produce" && std::string(argv[1]) != "consume")) {
            std::cerr << "usage: shm_ring_bench_peer <produce|consume> <name> <count> <payload-bytes>\n";
            return 2;
        }
        const auto count = parse(argv[3]);
        const auto payload_size = parse(argv[4]);
        auto session = shm_ring_lab::Session::open(argv[2]);
        shm_ring_lab::Message payload(payload_size, static_cast<std::uint8_t>('x'));
        const auto start = std::chrono::steady_clock::now();
        if (std::string(argv[1]) == "produce") {
            check(session.claim_writer(), "claim_writer");
            for (std::uint32_t i = 0; i < count; ++i) check(session.send(payload, 30s), "send");
            check(session.close_writer(), "close_writer");
        } else {
            check(session.claim_reader(), "claim_reader");
            for (std::uint32_t i = 0; i < count; ++i) {
                auto result = session.receive(30s);
                check(result.status, "receive");
            }
        }
        const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        const auto mib = (static_cast<double>(count) * payload_size) / (1024.0 * 1024.0);
        std::cout << argv[1] << ": " << count << " messages, " << payload_size << " bytes/message, "
                  << elapsed << " s, " << (elapsed > 0.0 ? count / elapsed : 0.0)
                  << " messages/s, " << (elapsed > 0.0 ? mib / elapsed : 0.0) << " MiB/s\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "shm_ring_bench_peer: " << error.what() << '\n';
        return 1;
    }
}
