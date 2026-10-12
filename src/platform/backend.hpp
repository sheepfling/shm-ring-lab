#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace shm_ring_lab::detail::platform {

struct Mapping {
    void* address = nullptr;
    std::size_t mapped_size = 0;
    std::intptr_t object_handle = -1;
    void* sync_handle = nullptr;
};

enum class LockResult { acquired, timeout, abandoned, failed };

class NotReady final : public std::runtime_error {
public:
    explicit NotReady(const std::string& message) : std::runtime_error(message) {}
};

Mapping create(const std::string& canonical_name, std::size_t bytes);
Mapping open(const std::string& canonical_name, std::size_t initial_bytes);
bool ensure_mapped_size(Mapping& mapping, std::size_t required_bytes) noexcept;
void close(Mapping& mapping) noexcept;
bool remove(const std::string& canonical_name) noexcept;
LockResult lock_for(Mapping& mapping, std::chrono::milliseconds timeout) noexcept;
void unlock(Mapping& mapping) noexcept;
std::uint64_t process_id() noexcept;
const char* backend_name() noexcept;

} // namespace shm_ring_lab::detail::platform
