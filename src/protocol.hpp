#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

namespace shm_ring_lab::detail {

constexpr std::uint64_t kProtocolMagic = 0x53484D52494E4731ULL; // "SHMRING1"
constexpr std::uint32_t kProtocolVersion = 1;
constexpr std::uint32_t kMaxCapacity = 4096;
constexpr std::uint32_t kMaxPayloadBytes = 1024 * 1024;
constexpr std::size_t kMaxMappingBytes = 64U * 1024U * 1024U;

// V1 shared data contains only fixed-width scalar fields and byte arrays.
// Native padding is guarded by header_size; participants must use a matching
// executable ABI. OS synchronization handles and local pointers never live here.
struct SharedHeader {
    std::uint64_t magic;
    std::uint32_t protocol_version;
    std::uint32_t header_size;
    std::uint64_t mapping_size;
    std::uint32_t capacity;
    std::uint32_t max_payload_bytes;
    std::uint32_t read_index;
    std::uint32_t write_index;
    std::uint32_t count;
    std::uint32_t writer_closed;
    std::uint32_t failed;
    std::uint32_t reserved;
    std::uint64_t writer_pid;
    std::uint64_t reader_pid;
};

struct SlotHeader {
    std::uint32_t size;
};

inline bool compute_mapping_size(std::uint32_t capacity,
                                std::uint32_t max_payload_bytes,
                                std::size_t& result) noexcept {
    const std::size_t per_slot = sizeof(SlotHeader) + max_payload_bytes;
    if (capacity == 0 || per_slot > (std::numeric_limits<std::size_t>::max() - sizeof(SharedHeader)) / capacity) {
        return false;
    }
    result = sizeof(SharedHeader) + static_cast<std::size_t>(capacity) * per_slot;
    return result <= kMaxMappingBytes;
}

inline std::size_t slot_offset(std::uint32_t index,
                               std::uint32_t max_payload_bytes) noexcept {
    return sizeof(SharedHeader) + static_cast<std::size_t>(index) *
        (sizeof(SlotHeader) + max_payload_bytes);
}

} // namespace shm_ring_lab::detail
