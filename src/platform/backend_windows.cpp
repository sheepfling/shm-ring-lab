#define NOMINMAX
#include <windows.h>

#include "backend.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>

namespace shm_ring_lab::detail::platform {
namespace {

std::wstring mapping_name(const std::string& name) {
    return L"Local\\srl_" + std::wstring(name.begin(), name.end());
}

std::wstring mutex_name(const std::string& name) {
    return L"Local\\srlm_" + std::wstring(name.begin(), name.end());
}

std::string windows_error(const char* operation, DWORD error) {
    return std::string(operation) + " failed (Windows error " + std::to_string(error) + ")";
}

HANDLE as_handle(std::intptr_t value) noexcept {
    return reinterpret_cast<HANDLE>(value);
}

std::intptr_t as_integer(HANDLE value) noexcept {
    return reinterpret_cast<std::intptr_t>(value);
}

} // namespace

Mapping create(const std::string& canonical_name, std::size_t bytes) {
    if (bytes == 0 || bytes > 64U * 1024U * 1024U) {
        throw std::runtime_error("requested shared-memory region is outside the 64 MiB limit");
    }
    ::SetLastError(ERROR_SUCCESS);
    const auto object = ::CreateFileMappingW(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
        static_cast<DWORD>((static_cast<std::uint64_t>(bytes) >> 32U) & 0xffffffffU),
        static_cast<DWORD>(static_cast<std::uint64_t>(bytes) & 0xffffffffU),
        mapping_name(canonical_name).c_str());
    if (object == nullptr) {
        throw std::runtime_error(windows_error("CreateFileMappingW", ::GetLastError()));
    }
    if (::GetLastError() == ERROR_ALREADY_EXISTS) {
        ::CloseHandle(object);
        throw std::runtime_error("session name already exists");
    }
    Mapping mapping;
    mapping.object_handle = as_integer(object);
    mapping.address = ::MapViewOfFile(object, FILE_MAP_ALL_ACCESS, 0, 0, bytes);
    if (mapping.address == nullptr) {
        const DWORD error = ::GetLastError();
        ::CloseHandle(object);
        throw std::runtime_error(windows_error("MapViewOfFile(create)", error));
    }
    mapping.mapped_size = bytes;
    ::SetLastError(ERROR_SUCCESS);
    const auto mutex = ::CreateMutexW(nullptr, FALSE, mutex_name(canonical_name).c_str());
    if (mutex == nullptr) {
        const DWORD error = ::GetLastError();
        ::UnmapViewOfFile(mapping.address);
        ::CloseHandle(object);
        throw std::runtime_error(windows_error("CreateMutexW", error));
    }
    if (::GetLastError() == ERROR_ALREADY_EXISTS) {
        ::CloseHandle(mutex);
        ::UnmapViewOfFile(mapping.address);
        ::CloseHandle(object);
        throw std::runtime_error("session synchronization name already exists");
    }
    mapping.sync_handle = mutex;
    return mapping;
}

Mapping open(const std::string& canonical_name, std::size_t initial_bytes) {
    if (initial_bytes == 0 || initial_bytes > 64U * 1024U * 1024U) {
        throw std::runtime_error("invalid initial mapping size");
    }
    const auto object = ::OpenFileMappingW(FILE_MAP_ALL_ACCESS, FALSE, mapping_name(canonical_name).c_str());
    if (object == nullptr) {
        const DWORD error = ::GetLastError();
        if (error == ERROR_FILE_NOT_FOUND) throw NotReady("mapping name is not published yet");
        throw std::runtime_error(windows_error("OpenFileMappingW", error));
    }
    Mapping mapping;
    mapping.object_handle = as_integer(object);
    mapping.address = ::MapViewOfFile(object, FILE_MAP_ALL_ACCESS, 0, 0, initial_bytes);
    if (mapping.address == nullptr) {
        const DWORD error = ::GetLastError();
        ::CloseHandle(object);
        throw std::runtime_error(windows_error("MapViewOfFile(open)", error));
    }
    mapping.mapped_size = initial_bytes;
    const auto mutex = ::OpenMutexW(SYNCHRONIZE | MUTEX_MODIFY_STATE, FALSE, mutex_name(canonical_name).c_str());
    if (mutex == nullptr) {
        const DWORD error = ::GetLastError();
        ::UnmapViewOfFile(mapping.address);
        ::CloseHandle(object);
        if (error == ERROR_FILE_NOT_FOUND) throw NotReady("mutex name is not published yet");
        throw std::runtime_error(windows_error("OpenMutexW", error));
    }
    mapping.sync_handle = mutex;
    return mapping;
}

bool ensure_mapped_size(Mapping& mapping, std::size_t required_bytes) noexcept {
    if (required_bytes == 0 || required_bytes > 64U * 1024U * 1024U ||
        mapping.object_handle == -1 || mapping.address == nullptr) {
        return false;
    }
    if (required_bytes <= mapping.mapped_size) return true;
    ::UnmapViewOfFile(mapping.address);
    mapping.address = nullptr;
    mapping.mapped_size = 0;
    mapping.address = ::MapViewOfFile(as_handle(mapping.object_handle), FILE_MAP_ALL_ACCESS,
                                      0, 0, required_bytes);
    if (mapping.address == nullptr) return false;
    mapping.mapped_size = required_bytes;
    return true;
}

void close(Mapping& mapping) noexcept {
    if (mapping.sync_handle != nullptr) {
        ::CloseHandle(static_cast<HANDLE>(mapping.sync_handle));
        mapping.sync_handle = nullptr;
    }
    if (mapping.address != nullptr) {
        ::UnmapViewOfFile(mapping.address);
        mapping.address = nullptr;
    }
    if (mapping.object_handle != -1) {
        ::CloseHandle(as_handle(mapping.object_handle));
        mapping.object_handle = -1;
    }
    mapping.mapped_size = 0;
}

bool remove(const std::string&) noexcept {
    // Named kernel objects disappear when the last process closes its handles.
    return true;
}

LockResult lock_for(Mapping& mapping, std::chrono::milliseconds timeout) noexcept {
    if (mapping.sync_handle == nullptr) return LockResult::failed;
    auto milliseconds = timeout.count() <= 0 ? 0ULL : static_cast<unsigned long long>(timeout.count());
    milliseconds = std::min(milliseconds, static_cast<unsigned long long>(INFINITE - 1U));
    const DWORD result = ::WaitForSingleObject(static_cast<HANDLE>(mapping.sync_handle), static_cast<DWORD>(milliseconds));
    if (result == WAIT_OBJECT_0) return LockResult::acquired;
    if (result == WAIT_ABANDONED) return LockResult::abandoned;
    if (result == WAIT_TIMEOUT) return LockResult::timeout;
    return LockResult::failed;
}

void unlock(Mapping& mapping) noexcept {
    if (mapping.sync_handle != nullptr) {
        (void)::ReleaseMutex(static_cast<HANDLE>(mapping.sync_handle));
    }
}

std::uint64_t process_id() noexcept {
    return static_cast<std::uint64_t>(::GetCurrentProcessId());
}

const char* backend_name() noexcept {
    return "Win32 file mapping + named mutex";
}

} // namespace shm_ring_lab::detail::platform
