#include "shm_ring_lab/session.hpp"

#include "platform/backend.hpp"
#include "protocol.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>

namespace shm_ring_lab {
namespace {

using Clock = std::chrono::steady_clock;
constexpr auto kMaximumOperationTimeout = std::chrono::hours(24);
constexpr auto kPollInterval = std::chrono::milliseconds(1);

std::string canonicalize_name(const std::string& name) {
    // 24 characters keeps the derived POSIX shm/semaphore names within the
    // short-name limits used by older Darwin systems.
    if (name.empty() || name.size() > 24) {
        throw std::invalid_argument("session name must contain 1 to 24 ASCII letters, digits, '_' or '-'");
    }
    std::string canonical;
    canonical.reserve(name.size());
    for (unsigned char ch : name) {
        const bool upper = ch >= 'A' && ch <= 'Z';
        const bool lower = ch >= 'a' && ch <= 'z';
        const bool digit = ch >= '0' && ch <= '9';
        if (!(upper || lower || digit || ch == '_' || ch == '-')) {
            throw std::invalid_argument("session name must contain only ASCII letters, digits, '_' or '-'");
        }
        canonical.push_back(static_cast<char>(upper ? ch + ('a' - 'A') : ch));
    }
    return canonical;
}

Clock::time_point deadline_from(std::chrono::milliseconds timeout) noexcept {
    if (timeout < std::chrono::milliseconds::zero()) {
        timeout = std::chrono::milliseconds::zero();
    }
    const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(kMaximumOperationTimeout);
    timeout = std::min(timeout, maximum);
    return Clock::now() + timeout;
}

std::chrono::milliseconds bounded_timeout(std::chrono::milliseconds timeout) noexcept {
    if (timeout < std::chrono::milliseconds::zero()) return std::chrono::milliseconds::zero();
    const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(kMaximumOperationTimeout);
    return std::min(timeout, maximum);
}

std::chrono::milliseconds remaining_until(Clock::time_point deadline) noexcept {
    const auto now = Clock::now();
    if (now >= deadline) {
        return std::chrono::milliseconds::zero();
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    if (remaining == std::chrono::milliseconds::zero()) {
        remaining = std::chrono::milliseconds(1);
    }
    return remaining;
}

detail::platform::LockResult lock_until(detail::platform::Mapping& mapping,
                                        Clock::time_point deadline) noexcept {
    for (;;) {
        const bool expired = Clock::now() >= deadline;
        const auto remaining = expired ? std::chrono::milliseconds::zero() : remaining_until(deadline);
        const auto slice = expired ? std::chrono::milliseconds::zero() :
            std::min(remaining, std::chrono::milliseconds(10));
        const auto result = detail::platform::lock_for(mapping, slice);
        if (result != detail::platform::LockResult::timeout || expired) return result;
    }
}

struct Guard {
    detail::platform::Mapping* mapping = nullptr;
    explicit Guard(detail::platform::Mapping& value) noexcept : mapping(&value) {}
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    ~Guard() { if (mapping != nullptr) detail::platform::unlock(*mapping); }
};

Status claim(detail::platform::Mapping& mapping,
             detail::SharedHeader& header,
             std::uint64_t& owner_pid,
             std::chrono::milliseconds timeout) {
    const auto lock_result = lock_until(mapping, deadline_from(bounded_timeout(timeout)));
    if (lock_result == detail::platform::LockResult::timeout) {
        return Status::timeout;
    }
    if (lock_result == detail::platform::LockResult::failed) {
        return Status::peer_failed;
    }
    Guard guard(mapping);
    if (lock_result == detail::platform::LockResult::abandoned) {
        header.failed = 1;
        return Status::peer_failed;
    }
    if (header.failed != 0) {
        return Status::peer_failed;
    }
    const auto current_pid = detail::platform::process_id();
    if (owner_pid == 0) {
        owner_pid = current_pid;
        return Status::ok;
    }
    return owner_pid == current_pid ? Status::ok : Status::role_in_use;
}

void validate_header(const detail::platform::Mapping& mapping) {
    if (mapping.address == nullptr || mapping.mapped_size < sizeof(detail::SharedHeader)) {
        throw std::runtime_error("shared-memory region is smaller than a protocol header");
    }
    const auto& header = *static_cast<const detail::SharedHeader*>(mapping.address);
    if (header.magic != detail::kProtocolMagic ||
        header.protocol_version != detail::kProtocolVersion ||
        header.header_size != sizeof(detail::SharedHeader)) {
        throw std::runtime_error("shared-memory protocol header is missing or incompatible");
    }
    if (header.capacity < 2 || header.capacity > detail::kMaxCapacity ||
        header.max_payload_bytes == 0 || header.max_payload_bytes > detail::kMaxPayloadBytes) {
        throw std::runtime_error("shared-memory protocol has invalid capacity limits");
    }
    std::size_t required_size = 0;
    if (!detail::compute_mapping_size(header.capacity, header.max_payload_bytes, required_size) ||
        header.mapping_size != required_size || required_size > mapping.mapped_size) {
        throw std::runtime_error("shared-memory protocol layout size is invalid");
    }
}

} // namespace

struct Session::Impl {
    detail::platform::Mapping mapping;
    detail::SharedHeader* header = nullptr;
    Options configured_options{};

    explicit Impl(detail::platform::Mapping value) : mapping(value) {
        header = static_cast<detail::SharedHeader*>(mapping.address);
        configured_options.capacity = header->capacity;
        configured_options.max_payload_bytes = header->max_payload_bytes;
    }
    ~Impl() { detail::platform::close(mapping); }
};

Session::Session(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
Session::Session(Session&&) noexcept = default;
Session& Session::operator=(Session&&) noexcept = default;
Session::~Session() = default;

Session Session::create(const std::string& name, Options options) {
    const auto canonical_name = canonicalize_name(name);
    if (options.capacity < 2 || options.capacity > detail::kMaxCapacity) {
        throw std::invalid_argument("capacity must be between 2 and 4096 slots");
    }
    if (options.max_payload_bytes == 0 || options.max_payload_bytes > detail::kMaxPayloadBytes) {
        throw std::invalid_argument("max_payload_bytes must be between 1 and 1048576");
    }
    std::size_t mapping_size = 0;
    if (!detail::compute_mapping_size(options.capacity, options.max_payload_bytes, mapping_size)) {
        throw std::invalid_argument("requested ring exceeds the 64 MiB mapping limit");
    }

    auto mapping = detail::platform::create(canonical_name, mapping_size);
    auto* header = new (mapping.address) detail::SharedHeader{};
    header->magic = detail::kProtocolMagic;
    header->protocol_version = detail::kProtocolVersion;
    header->header_size = static_cast<std::uint32_t>(sizeof(detail::SharedHeader));
    header->mapping_size = mapping_size;
    header->capacity = options.capacity;
    header->max_payload_bytes = options.max_payload_bytes;
    return Session(std::make_unique<Impl>(mapping));
}

Session Session::open(const std::string& name) {
    const auto canonical_name = canonicalize_name(name);
    auto mapping = detail::platform::open(canonical_name);
    try {
        validate_header(mapping);
    } catch (...) {
        detail::platform::close(mapping);
        throw;
    }
    return Session(std::make_unique<Impl>(mapping));
}

bool Session::remove(const std::string& name) noexcept {
    try {
        return detail::platform::remove(canonicalize_name(name));
    } catch (...) {
        return false;
    }
}

Status Session::claim_writer(std::chrono::milliseconds timeout) {
    if (!impl_) return Status::invalid_argument;
    return claim(impl_->mapping, *impl_->header, impl_->header->writer_pid, timeout);
}

Status Session::claim_reader(std::chrono::milliseconds timeout) {
    if (!impl_) return Status::invalid_argument;
    return claim(impl_->mapping, *impl_->header, impl_->header->reader_pid, timeout);
}

Status Session::send(const Message& message, std::chrono::milliseconds timeout) {
    return send(message.data(), message.size(), timeout);
}

Status Session::send(const void* data, std::size_t size, std::chrono::milliseconds timeout) {
    if (!impl_) return Status::invalid_argument;
    if (size > impl_->configured_options.max_payload_bytes) return Status::too_large;
    if (size != 0 && data == nullptr) return Status::invalid_argument;

    const auto deadline = deadline_from(timeout);
    for (;;) {
        const auto lock_result = lock_until(impl_->mapping, deadline);
        if (lock_result == detail::platform::LockResult::timeout) return Status::timeout;
        if (lock_result == detail::platform::LockResult::failed) return Status::peer_failed;
        Guard guard(impl_->mapping);
        if (lock_result == detail::platform::LockResult::abandoned) {
            impl_->header->failed = 1;
            return Status::peer_failed;
        }
        auto& header = *impl_->header;
        if (header.failed != 0) return Status::peer_failed;
        if (header.read_index >= header.capacity || header.write_index >= header.capacity ||
            header.count > header.capacity) {
            header.failed = 1;
            return Status::invalid_frame;
        }
        const auto current_pid = detail::platform::process_id();
        if (header.writer_pid == 0) header.writer_pid = current_pid;
        if (header.writer_pid != current_pid) return Status::role_in_use;
        if (header.writer_closed != 0) return Status::closed;

        if (header.count < header.capacity) {
            const auto offset = detail::slot_offset(header.write_index, header.max_payload_bytes);
            auto* slot = static_cast<std::uint8_t*>(impl_->mapping.address) + offset;
            const auto payload_size = static_cast<std::uint32_t>(size);
            std::memcpy(slot, &payload_size, sizeof(payload_size));
            if (size != 0) {
                std::memcpy(slot + sizeof(payload_size), data, size);
            }
            header.write_index = (header.write_index + 1U) % header.capacity;
            ++header.count;
            return Status::ok;
        }
        guard.mapping = nullptr;
        detail::platform::unlock(impl_->mapping);
        if (Clock::now() >= deadline) return Status::timeout;
        std::this_thread::sleep_for(std::min(kPollInterval, remaining_until(deadline)));
    }
}

ReceiveResult Session::receive(std::chrono::milliseconds timeout) {
    if (!impl_) return {Status::invalid_argument, {}};
    const auto deadline = deadline_from(timeout);
    for (;;) {
        const auto lock_result = lock_until(impl_->mapping, deadline);
        if (lock_result == detail::platform::LockResult::timeout) return {Status::timeout, {}};
        if (lock_result == detail::platform::LockResult::failed) return {Status::peer_failed, {}};
        Guard guard(impl_->mapping);
        auto& header = *impl_->header;
        if (lock_result == detail::platform::LockResult::abandoned) {
            header.failed = 1;
            return {Status::peer_failed, {}};
        }
        if (header.failed != 0) return {Status::peer_failed, {}};
        if (header.read_index >= header.capacity || header.write_index >= header.capacity ||
            header.count > header.capacity) {
            header.failed = 1;
            return {Status::invalid_frame, {}};
        }
        const auto current_pid = detail::platform::process_id();
        if (header.reader_pid == 0) header.reader_pid = current_pid;
        if (header.reader_pid != current_pid) return {Status::role_in_use, {}};

        if (header.count != 0) {
            const auto offset = detail::slot_offset(header.read_index, header.max_payload_bytes);
            auto* slot = static_cast<std::uint8_t*>(impl_->mapping.address) + offset;
            std::uint32_t payload_size = 0;
            std::memcpy(&payload_size, slot, sizeof(payload_size));
            if (payload_size > header.max_payload_bytes) {
                header.failed = 1;
                return {Status::invalid_frame, {}};
            }
            Message message(payload_size);
            if (payload_size != 0) {
                std::memcpy(message.data(), slot + sizeof(payload_size), payload_size);
            }
            header.read_index = (header.read_index + 1U) % header.capacity;
            --header.count;
            return {Status::ok, std::move(message)};
        }
        if (header.writer_closed != 0) return {Status::closed, {}};

        guard.mapping = nullptr;
        detail::platform::unlock(impl_->mapping);
        if (Clock::now() >= deadline) return {Status::timeout, {}};
        std::this_thread::sleep_for(std::min(kPollInterval, remaining_until(deadline)));
    }
}

Status Session::close_writer(std::chrono::milliseconds timeout) {
    if (!impl_) return Status::invalid_argument;
    const auto lock_result = lock_until(impl_->mapping, deadline_from(bounded_timeout(timeout)));
    if (lock_result == detail::platform::LockResult::timeout) return Status::timeout;
    if (lock_result == detail::platform::LockResult::failed) return Status::peer_failed;
    Guard guard(impl_->mapping);
    auto& header = *impl_->header;
    if (lock_result == detail::platform::LockResult::abandoned) {
        header.failed = 1;
        return Status::peer_failed;
    }
    if (header.failed != 0) return Status::peer_failed;
    const auto current_pid = detail::platform::process_id();
    if (header.writer_pid == 0) header.writer_pid = current_pid;
    if (header.writer_pid != current_pid) return Status::role_in_use;
    header.writer_closed = 1;
    return Status::ok;
}

Options Session::options() const noexcept {
    return impl_ ? impl_->configured_options : Options{};
}

const char* to_string(Status status) noexcept {
    switch (status) {
    case Status::ok: return "ok";
    case Status::timeout: return "timeout";
    case Status::closed: return "closed";
    case Status::too_large: return "too_large";
    case Status::invalid_argument: return "invalid_argument";
    case Status::invalid_frame: return "invalid_frame";
    case Status::role_in_use: return "role_in_use";
    case Status::peer_failed: return "peer_failed";
    }
    return "unknown";
}

} // namespace shm_ring_lab
