#include "backend.hpp"

#include <cerrno>
#include <fcntl.h>
#include <semaphore.h>
#include <algorithm>
#include <thread>
#include <chrono>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace shm_ring_lab::detail::platform {
namespace {

constexpr std::size_t kMaxRegionSize = 64U * 1024U * 1024U;

std::string shared_memory_name(const std::string& name) {
    return "/srl_" + name;
}

std::string semaphore_name(const std::string& name) {
    return "/srlm_" + name;
}

void discard_partial(Mapping& mapping, const std::string& name) noexcept {
    close(mapping);
    (void)::shm_unlink(shared_memory_name(name).c_str());
}

} // namespace

Mapping create(const std::string& canonical_name, std::size_t bytes) {
    if (bytes == 0 || bytes > kMaxRegionSize) {
        throw std::runtime_error("requested shared-memory region is outside the 64 MiB limit");
    }
    const auto shm_name = shared_memory_name(canonical_name);
    const auto sem_name = semaphore_name(canonical_name);
    Mapping mapping;
    const int fd = ::shm_open(shm_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) {
        throw std::runtime_error("shm_open(create) failed for session '" + canonical_name + "' (errno " + std::to_string(errno) + ")");
    }
    mapping.object_handle = fd;
    mapping.mapped_size = bytes;
    if (::ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
        const int error = errno;
        discard_partial(mapping, canonical_name);
        throw std::runtime_error("ftruncate failed (errno " + std::to_string(error) + ")");
    }
    mapping.address = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping.address == MAP_FAILED) {
        mapping.address = nullptr;
        const int error = errno;
        discard_partial(mapping, canonical_name);
        throw std::runtime_error("mmap failed (errno " + std::to_string(error) + ")");
    }
    auto* semaphore = ::sem_open(sem_name.c_str(), O_CREAT | O_EXCL, 0600, 1);
    if (semaphore == SEM_FAILED) {
        const int error = errno;
        discard_partial(mapping, canonical_name);
        throw std::runtime_error("sem_open(create) failed (errno " + std::to_string(error) + ")");
    }
    mapping.sync_handle = semaphore;
    return mapping;
}

Mapping open(const std::string& canonical_name) {
    const auto shm_name = shared_memory_name(canonical_name);
    const auto sem_name = semaphore_name(canonical_name);
    Mapping mapping;
    const int fd = ::shm_open(shm_name.c_str(), O_RDWR, 0600);
    if (fd < 0) {
        throw std::runtime_error("shm_open(open) failed for session '" + canonical_name + "' (errno " + std::to_string(errno) + ")");
    }
    mapping.object_handle = fd;
    struct stat info{};
    if (::fstat(fd, &info) != 0 || info.st_size <= 0 || static_cast<std::uint64_t>(info.st_size) > kMaxRegionSize) {
        const int error = errno;
        close(mapping);
        throw std::runtime_error("shared-memory file has an invalid size (errno " + std::to_string(error) + ")");
    }
    mapping.mapped_size = static_cast<std::size_t>(info.st_size);
    mapping.address = ::mmap(nullptr, mapping.mapped_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping.address == MAP_FAILED) {
        mapping.address = nullptr;
        const int error = errno;
        close(mapping);
        throw std::runtime_error("mmap(open) failed (errno " + std::to_string(error) + ")");
    }
    auto* semaphore = ::sem_open(sem_name.c_str(), 0);
    if (semaphore == SEM_FAILED) {
        const int error = errno;
        close(mapping);
        throw std::runtime_error("sem_open(open) failed (errno " + std::to_string(error) + ")");
    }
    mapping.sync_handle = semaphore;
    return mapping;
}

void close(Mapping& mapping) noexcept {
    if (mapping.sync_handle != nullptr) {
        (void)::sem_close(static_cast<sem_t*>(mapping.sync_handle));
        mapping.sync_handle = nullptr;
    }
    if (mapping.address != nullptr) {
        (void)::munmap(mapping.address, mapping.mapped_size);
        mapping.address = nullptr;
    }
    if (mapping.object_handle >= 0) {
        (void)::close(static_cast<int>(mapping.object_handle));
        mapping.object_handle = -1;
    }
    mapping.mapped_size = 0;
}

bool remove(const std::string& canonical_name) noexcept {
    bool okay = true;
    if (::shm_unlink(shared_memory_name(canonical_name).c_str()) != 0 && errno != ENOENT) okay = false;
    if (::sem_unlink(semaphore_name(canonical_name).c_str()) != 0 && errno != ENOENT) okay = false;
    return okay;
}

LockResult lock_for(Mapping& mapping, std::chrono::milliseconds timeout) noexcept {
    if (mapping.sync_handle == nullptr) return LockResult::failed;
    auto* semaphore = static_cast<sem_t*>(mapping.sync_handle);
    if (timeout < std::chrono::milliseconds::zero()) timeout = std::chrono::milliseconds::zero();
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        int result;
        do { result = ::sem_trywait(semaphore); } while (result != 0 && errno == EINTR);
        if (result == 0) return LockResult::acquired;
        if (errno != EAGAIN) return LockResult::failed;
        if (std::chrono::steady_clock::now() >= deadline) return LockResult::timeout;
        std::this_thread::sleep_for(std::min(std::chrono::milliseconds(1),
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now())));
    }
}

void unlock(Mapping& mapping) noexcept {
    if (mapping.sync_handle != nullptr) {
        (void)::sem_post(static_cast<sem_t*>(mapping.sync_handle));
    }
}

std::uint64_t process_id() noexcept {
    return static_cast<std::uint64_t>(::getpid());
}

const char* backend_name() noexcept {
#if defined(__APPLE__)
    return "POSIX shm_open + named semaphore (macOS)";
#else
    return "POSIX shm_open + named semaphore";
#endif
}

} // namespace shm_ring_lab::detail::platform
