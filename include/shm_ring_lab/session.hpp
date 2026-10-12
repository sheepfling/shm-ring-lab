#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace shm_ring_lab {

using Message = std::vector<std::uint8_t>;

struct Options {
    std::uint32_t capacity = 16;
    std::uint32_t max_payload_bytes = 4096;
};

enum class Status {
    ok,
    timeout,
    closed,
    too_large,
    invalid_argument,
    invalid_frame,
    role_in_use,
    peer_failed,
};

const char* to_string(Status status) noexcept;

struct ReceiveResult {
    Status status = Status::timeout;
    Message message;
};

// One Session describes one SPSC direction. Shared bytes are copied into and
// out of the bounded ring; no process-local pointers cross the mapping.
class Session {
public:
    static Session create(const std::string& name, Options options = {});
    static Session open(const std::string& name);
    // POSIX removes the names immediately; Windows names disappear after the
    // final mapping/mutex handles close. Call only after peers have stopped.
    static bool remove(const std::string& name) noexcept;

    Session(Session&&) noexcept;
    Session& operator=(Session&&) noexcept;
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    ~Session();

    // A session has one writer process and one reader process. Claiming early
    // is useful for readiness handshakes; send/receive also claim implicitly.
    Status claim_writer(std::chrono::milliseconds timeout = std::chrono::seconds(1));
    Status claim_reader(std::chrono::milliseconds timeout = std::chrono::seconds(1));

    Status send(const Message& message, std::chrono::milliseconds timeout);
    Status send(const void* data, std::size_t size, std::chrono::milliseconds timeout);
    ReceiveResult receive(std::chrono::milliseconds timeout);
    // Stops new writes. The reader drains queued messages before receiving
    // Status::closed.
    Status close_writer(std::chrono::milliseconds timeout = std::chrono::seconds(1));

    Options options() const noexcept;

private:
    struct Impl;
    explicit Session(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

} // namespace shm_ring_lab
