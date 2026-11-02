#include "shm_ring_lab/session.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace std::chrono_literals;
using shm_ring_lab::Session;
using shm_ring_lab::Status;

namespace {

void expect(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

std::string unique_name() {
    static std::uint32_t counter = 0;
#ifdef _WIN32
    const auto pid = static_cast<unsigned long long>(::GetCurrentProcessId());
#else
    const auto pid = static_cast<unsigned long long>(::getpid());
#endif
    return "srl" + std::to_string(pid) + "x" + std::to_string(++counter);
}

std::filesystem::path marker_path(const std::string& name) {
    return std::filesystem::temp_directory_path() / (name + ".ready");
}

void write_text_file(const std::filesystem::path& path, const std::string& contents) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    file.flush();
    expect(static_cast<bool>(file), "failed to write test input/handshake file " + path.string());
}

#ifdef _WIN32

std::wstring widen(const std::string& input) {
    return std::wstring(input.begin(), input.end());
}

std::wstring quote_argument(const std::wstring& arg) {
    std::wstring result = L"\"";
    std::size_t slashes = 0;
    for (wchar_t ch : arg) {
        if (ch == L'\\') {
            ++slashes;
        } else if (ch == L'\"') {
            result.append(slashes * 2 + 1, L'\\');
            result.push_back(L'\"');
            slashes = 0;
        } else {
            result.append(slashes, L'\\');
            result.push_back(ch);
            slashes = 0;
        }
    }
    result.append(slashes * 2, L'\\');
    result.push_back(L'\"');
    return result;
}

class Child {
public:
    Child(const std::string& executable, const std::vector<std::string>& args,
          bool watchdog_group = false, const std::filesystem::path& stdin_file = {}) {
        if (watchdog_group) {
            job_ = ::CreateJobObjectW(nullptr, nullptr);
            if (job_ == nullptr) throw std::runtime_error("CreateJobObjectW failed");
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if (!::SetInformationJobObject(job_, JobObjectExtendedLimitInformation,
                    &limits, sizeof(limits))) {
                const auto error = ::GetLastError();
                ::CloseHandle(job_);
                job_ = nullptr;
                throw std::runtime_error("SetInformationJobObject failed (" + std::to_string(error) + ")");
            }
        }
        std::wstring command = quote_argument(widen(executable));
        for (const auto& arg : args) command += L" " + quote_argument(widen(arg));
        std::vector<wchar_t> mutable_command(command.begin(), command.end());
        mutable_command.push_back(L'\0');
        STARTUPINFOW startup{};
        startup.cb = sizeof(startup);
        HANDLE input_handle = INVALID_HANDLE_VALUE;
        HANDLE null_handle = INVALID_HANDLE_VALUE;
        BOOL inherit_handles = FALSE;
        if (!stdin_file.empty()) {
            SECURITY_ATTRIBUTES attributes{};
            attributes.nLength = sizeof(attributes);
            attributes.bInheritHandle = TRUE;
            input_handle = ::CreateFileW(stdin_file.c_str(), GENERIC_READ,
                                         FILE_SHARE_READ, &attributes, OPEN_EXISTING,
                                         FILE_ATTRIBUTE_NORMAL, nullptr);
            if (input_handle == INVALID_HANDLE_VALUE) {
                const auto error = ::GetLastError();
                if (job_ != nullptr) ::CloseHandle(job_);
                job_ = nullptr;
                throw std::runtime_error("CreateFileW(stdin) failed (" + std::to_string(error) + ")");
            }
            null_handle = ::CreateFileW(L"NUL", GENERIC_WRITE,
                                        FILE_SHARE_READ | FILE_SHARE_WRITE, &attributes,
                                        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (null_handle == INVALID_HANDLE_VALUE) {
                const auto error = ::GetLastError();
                ::CloseHandle(input_handle);
                if (job_ != nullptr) ::CloseHandle(job_);
                job_ = nullptr;
                throw std::runtime_error("CreateFileW(NUL) failed (" + std::to_string(error) + ")");
            }
            startup.dwFlags = STARTF_USESTDHANDLES;
            startup.hStdInput = input_handle;
            startup.hStdOutput = null_handle;
            startup.hStdError = null_handle;
            inherit_handles = TRUE;
        }
        const DWORD flags = CREATE_NO_WINDOW | (job_ != nullptr ? CREATE_SUSPENDED : 0U);
        const BOOL created = ::CreateProcessW(nullptr, mutable_command.data(), nullptr, nullptr,
                                              inherit_handles, flags, nullptr, nullptr,
                                              &startup, &process_);
        const auto create_error = created ? ERROR_SUCCESS : ::GetLastError();
        if (input_handle != INVALID_HANDLE_VALUE) ::CloseHandle(input_handle);
        if (null_handle != INVALID_HANDLE_VALUE) ::CloseHandle(null_handle);
        if (!created) {
            if (job_ != nullptr) ::CloseHandle(job_);
            job_ = nullptr;
            throw std::runtime_error("CreateProcessW failed (" + std::to_string(create_error) + ")");
        }
        if (job_ != nullptr) {
            if (!::AssignProcessToJobObject(job_, process_.hProcess)) {
                const auto error = ::GetLastError();
                (void)::TerminateProcess(process_.hProcess, 137);
                (void)::WaitForSingleObject(process_.hProcess, 5000);
                ::CloseHandle(process_.hThread);
                ::CloseHandle(process_.hProcess);
                ::CloseHandle(job_);
                process_ = {};
                job_ = nullptr;
                throw std::runtime_error("AssignProcessToJobObject failed (" + std::to_string(error) + ")");
            }
            if (::ResumeThread(process_.hThread) == static_cast<DWORD>(-1)) {
                const auto error = ::GetLastError();
                (void)::TerminateJobObject(job_, 137);
                (void)::WaitForSingleObject(process_.hProcess, 5000);
                ::CloseHandle(process_.hThread);
                ::CloseHandle(process_.hProcess);
                ::CloseHandle(job_);
                process_ = {};
                job_ = nullptr;
                throw std::runtime_error("ResumeThread failed (" + std::to_string(error) + ")");
            }
        }
        ::CloseHandle(process_.hThread);
        process_.hThread = nullptr;
    }
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;
    ~Child() { terminate(); close_handles(); }

    bool wait_for(std::chrono::milliseconds timeout, int& exit_code) {
        const auto value = ::WaitForSingleObject(process_.hProcess, static_cast<DWORD>(timeout.count()));
        if (value == WAIT_TIMEOUT) return false;
        if (value != WAIT_OBJECT_0) throw std::runtime_error("WaitForSingleObject failed");
        DWORD code = 1;
        if (!::GetExitCodeProcess(process_.hProcess, &code)) throw std::runtime_error("GetExitCodeProcess failed");
        exit_code = static_cast<int>(code);
        finished_ = true;
        return true;
    }
    void terminate() noexcept {
        if (!finished_ && process_.hProcess != nullptr) {
            if (job_ != nullptr) (void)::TerminateJobObject(job_, 137);
            else (void)::TerminateProcess(process_.hProcess, 137);
            (void)::WaitForSingleObject(process_.hProcess, 5000);
            finished_ = true;
        }
    }

private:
    void close_handles() noexcept {
        if (process_.hProcess != nullptr) ::CloseHandle(process_.hProcess);
        process_.hProcess = nullptr;
        if (job_ != nullptr) ::CloseHandle(job_);
        job_ = nullptr;
    }
    PROCESS_INFORMATION process_{};
    HANDLE job_ = nullptr;
    bool finished_ = false;
};

#else

class Child {
public:
    Child(const std::string& executable, const std::vector<std::string>& args,
          bool watchdog_group = false, const std::filesystem::path& stdin_file = {})
        : watchdog_group_(watchdog_group) {
        pid_ = ::fork();
        if (pid_ < 0) throw std::runtime_error("fork failed");
        if (pid_ == 0) {
            if (watchdog_group_) (void)::setpgid(0, 0);
            if (!stdin_file.empty()) {
                const int input = ::open(stdin_file.c_str(), O_RDONLY);
                if (input < 0) ::_exit(126);
                if (input != STDIN_FILENO) {
                    if (::dup2(input, STDIN_FILENO) < 0) ::_exit(126);
                    (void)::close(input);
                }
                const int null_output = ::open("/dev/null", O_WRONLY);
                if (null_output < 0) ::_exit(126);
                if ((null_output != STDOUT_FILENO &&
                     ::dup2(null_output, STDOUT_FILENO) < 0) ||
                    (null_output != STDERR_FILENO &&
                     ::dup2(null_output, STDERR_FILENO) < 0)) {
                    ::_exit(126);
                }
                if (null_output != STDOUT_FILENO && null_output != STDERR_FILENO) {
                    (void)::close(null_output);
                }
            }
            std::vector<std::string> storage;
            storage.reserve(args.size() + 1);
            storage.push_back(executable);
            storage.insert(storage.end(), args.begin(), args.end());
            std::vector<char*> argv;
            argv.reserve(storage.size() + 1);
            for (auto& item : storage) argv.push_back(item.data());
            argv.push_back(nullptr);
            ::execv(executable.c_str(), argv.data());
            ::_exit(127);
        }
        if (watchdog_group_) (void)::setpgid(pid_, pid_);
    }
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;
    ~Child() { terminate(); }

    bool wait_for(std::chrono::milliseconds timeout, int& exit_code) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            int status = 0;
            const auto result = ::waitpid(pid_, &status, WNOHANG);
            if (result == pid_) {
                finished_ = true;
                exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
                return true;
            }
            if (result < 0 && errno != EINTR) throw std::runtime_error("waitpid failed");
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(5ms);
        }
    }
    void terminate() noexcept {
        if (!finished_ && pid_ > 0) {
            (void)::kill(watchdog_group_ ? -pid_ : pid_, SIGKILL);
            const auto deadline = std::chrono::steady_clock::now() + 5s;
            for (;;) {
                int status = 0;
                const auto result = ::waitpid(pid_, &status, WNOHANG);
                if (result == pid_ || (result < 0 && errno != EINTR)) break;
                if (std::chrono::steady_clock::now() >= deadline) {
                    std::fprintf(stderr,
                                 "child %ld did not reap within 5 seconds after SIGKILL; "
                                 "leaving final cleanup to the operating system\n",
                                 static_cast<long>(pid_));
                    break;
                }
                std::this_thread::sleep_for(5ms);
            }
            finished_ = true;
        }
    }

private:
    pid_t pid_ = -1;
    bool finished_ = false;
    bool watchdog_group_ = false;
};

#endif

void wait_success(Child& child, std::chrono::milliseconds timeout, const char* description) {
    int code = -1;
    expect(child.wait_for(timeout, code), std::string(description) + " exceeded its process deadline");
    expect(code == 0, std::string(description) + " exited with code " + std::to_string(code));
}

void wait_marker(const std::filesystem::path& path) {
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!std::filesystem::exists(path)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error("child readiness handshake exceeded 3 seconds");
        }
        std::this_thread::sleep_for(5ms);
    }
}

void test_fifo_wraparound_and_shutdown(const std::string& peer) {
    const auto name = unique_name();
    {
        auto owner = Session::create(name, {4, 32});
        Child consumer(peer, {"consume", name, "300", "item-"});
        std::this_thread::sleep_for(30ms);
        Child producer(peer, {"produce", name, "300", "item-"});
        wait_success(producer, 12s, "producer");
        wait_success(consumer, 12s, "consumer");
    }
    expect(Session::remove(name), "cleanup after FIFO test failed");
}

void test_empty_timeout(const std::string& peer) {
    const auto name = unique_name();
    const auto marker = marker_path(name);
    {
        auto owner = Session::create(name, {2, 32});
        Child consumer(peer, {"empty-wait", name, marker.string()});
        wait_marker(marker);
        Child producer(peer, {"produce", name, "1", "wake"});
        wait_success(producer, 5s, "empty-timeout producer");
        wait_success(consumer, 5s, "empty-timeout consumer");
    }
    std::filesystem::remove(marker);
    expect(Session::remove(name), "cleanup after empty test failed");
}

void test_full_timeout_and_idle_peer_death(const std::string& peer) {
    const auto name = unique_name();
    const auto marker = marker_path(name);
    {
        auto session = Session::create(name, {2, 4});
        expect(session.claim_writer() == Status::ok, "writer role claim failed");
        Child consumer(peer, {"claim-wait", name, marker.string()});
        wait_marker(marker);
        const std::uint8_t value = 7;
        expect(session.send(&value, 1, 500ms) == Status::ok, "first full-test send failed");
        expect(session.send(&value, 1, 500ms) == Status::ok, "second full-test send failed");
        const auto start = std::chrono::steady_clock::now();
        expect(session.send(&value, 1, 150ms) == Status::timeout, "full ring did not apply backpressure");
        expect(std::chrono::steady_clock::now() - start < 1s, "full-ring timeout was not bounded");
        consumer.terminate(); // registered reader exits without draining the full ring
        const auto dead_start = std::chrono::steady_clock::now();
        expect(session.send(&value, 1, 150ms) == Status::timeout, "dead idle reader did not remain bounded");
        expect(std::chrono::steady_clock::now() - dead_start < 1s, "dead-peer send exceeded its deadline");
    }
    std::filesystem::remove(marker);
    expect(Session::remove(name), "cleanup after peer-death test failed");
}

void test_dead_lock_owner_requires_recreate(const std::string& peer) {
    const auto name = unique_name();
    const auto replacement = unique_name();
    const auto marker = marker_path(name);
    {
        auto owner = Session::create(name, {2, 16});
        Child crashed(peer, {"crash-lock", name, marker.string()});
        wait_marker(marker);
        int code = -1;
        expect(crashed.wait_for(3s, code) && code == 0, "lock-owner crash helper did not exit");
        const auto start = std::chrono::steady_clock::now();
        bool rejected = false;
        try {
            auto opened = Session::open(name);
            (void)opened;
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        expect(rejected, "abandoned synchronization state was treated as usable");
        expect(std::chrono::steady_clock::now() - start < 3s,
               "dead synchronization owner blocked beyond the bound");
    }
    std::filesystem::remove(marker);
    expect(Session::remove(name), "cleanup after dead-lock-owner test failed");
    {
        auto fresh = Session::create(replacement, {2, 16});
        const std::uint8_t value = 4;
        expect(fresh.send(&value, 1, 200ms) == Status::ok,
               "fresh-session recreation policy did not restore operation");
    }
    expect(Session::remove(replacement), "cleanup of replacement session failed");
}

void test_initialization_gate(const std::string& peer) {
    const auto name = unique_name();
    const auto marker = marker_path(name);
    {
        Child initializer(peer, {"partial-create", name, marker.string(), "300"});
        wait_marker(marker);
        const auto start = std::chrono::steady_clock::now();
        auto opened = Session::open(name);
        expect(opened.options().capacity == 2 && opened.options().max_payload_bytes == 16,
               "opener observed incomplete creator options");
        expect(std::chrono::steady_clock::now() - start < 2s,
               "opener exceeded the initialization deadline");
        wait_success(initializer, 2s, "delayed initializer");
    }
    std::filesystem::remove(marker);
    expect(Session::remove(name), "cleanup after initialization race failed");

    const auto aborted_name = unique_name();
    const auto aborted_marker = marker_path(aborted_name);
    Child abandoned_creator(peer, {"partial-abort", aborted_name, aborted_marker.string()});
    wait_marker(aborted_marker);
    const auto start = std::chrono::steady_clock::now();
    bool rejected = false;
    try {
        auto incomplete = Session::open(aborted_name);
        (void)incomplete;
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    expect(rejected, "incomplete shared-memory header was accepted");
    expect(std::chrono::steady_clock::now() - start < 3s,
           "incomplete initialization wait exceeded its bound");
    abandoned_creator.terminate();
    std::filesystem::remove(aborted_marker);
    expect(Session::remove(aborted_name), "cleanup after aborted initialization failed");
}

void test_role_ownership(const std::string& peer) {
    for (const auto* role : {"writer", "reader"}) {
        const auto name = unique_name();
        const auto marker = marker_path(name);
        {
            auto owner = Session::create(name, {2, 16});
            Child holder(peer, {"hold-role", name, role, marker.string()});
            wait_marker(marker);
            Child competitor(peer, {"compete-role", name, role});
            wait_success(competitor, 3s, "duplicate-role competitor");
            holder.terminate();
        }
        std::filesystem::remove(marker);
        expect(Session::remove(name), std::string("cleanup after duplicate ") + role + " test failed");
    }
}

void test_size_and_frame_validation(const std::string& peer) {
    const auto collision_name = unique_name();
    {
        auto first = Session::create(collision_name, {2, 4});
        bool collision_rejected = false;
        try {
            auto duplicate = Session::create(collision_name, {2, 4});
            (void)duplicate;
        } catch (const std::runtime_error&) {
            collision_rejected = true;
        }
        expect(collision_rejected, "exclusive create reused an active name");
        expect(first.send(nullptr, 0, 100ms) == Status::ok,
               "failed duplicate create damaged the original session");
    }
    expect(Session::remove(collision_name), "cleanup after exclusive-create test failed");

    const auto case_name = unique_name();
    auto mixed_case_name = case_name;
    mixed_case_name.front() = 'S';
    {
        auto owner = Session::create(mixed_case_name, {2, 4});
        auto opener = Session::open(case_name);
        expect(opener.options().capacity == 2, "same-name opener rejected a valid session");
    }
    expect(Session::remove(case_name), "cleanup after name-normalization test failed");

    const auto size_name = unique_name();
    {
        auto session = Session::create(size_name, {2, 4});
        const std::uint8_t oversize[5] = {1, 2, 3, 4, 5};
        expect(session.send(oversize, sizeof(oversize), 10ms) == Status::too_large,
               "oversized outgoing frame was accepted");
        expect(session.send(nullptr, 1, 10ms) == Status::invalid_argument,
               "null payload with nonzero length was accepted");
        expect(session.send(nullptr, 0, 10ms) == Status::ok,
               "zero-byte frame was rejected");
    }
    expect(Session::remove(size_name), "cleanup after size test failed");

    const auto frame_name = unique_name();
    {
        auto owner = Session::create(frame_name, {2, 4});
        Child corrupter(peer, {"corrupt-frame", frame_name});
        wait_success(corrupter, 3s, "invalid-frame injector");
        auto reader = Session::open(frame_name);
        const auto frame = reader.receive(100ms);
        expect(frame.status == Status::invalid_frame,
               "oversized frame from shared memory was not rejected");
        expect(reader.receive(10ms).status == Status::peer_failed,
               "session was not failed after invalid shared frame");
    }
    expect(Session::remove(frame_name), "cleanup after invalid-frame test failed");

    const auto version_name = unique_name();
    {
        auto owner = Session::create(version_name, {2, 4});
        Child corrupter(peer, {"corrupt-version", version_name});
        wait_success(corrupter, 3s, "protocol-version injector");
        bool rejected = false;
        try {
            auto incompatible = Session::open(version_name);
            (void)incompatible;
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        expect(rejected, "incompatible protocol version was accepted");
    }
    expect(Session::remove(version_name), "cleanup after version test failed");

    const auto layout_name = unique_name();
    {
        auto owner = Session::create(layout_name, {2, 4});
        auto already_open = Session::open(layout_name);
        Child corrupter(peer, {"corrupt-layout", layout_name});
        wait_success(corrupter, 3s, "protocol-layout injector");
        expect(owner.send(nullptr, 0, 100ms) == Status::invalid_frame,
               "already-open session used a corrupted layout");
        expect(already_open.send(nullptr, 0, 100ms) == Status::peer_failed,
               "failed session was not rejected after corrupted layout");
        bool rejected = false;
        try {
            auto incompatible = Session::open(layout_name);
            (void)incompatible;
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        expect(rejected, "hostile declared mapping size was accepted");
    }
    expect(Session::remove(layout_name), "cleanup after layout test failed");

    const auto missing_name = unique_name();
    std::string missing_error;
    const auto missing_start = std::chrono::steady_clock::now();
    try {
        auto missing = Session::open(missing_name);
        (void)missing;
    } catch (const std::runtime_error& error) {
        missing_error = error.what();
    }
    expect(missing_error == "timed out waiting for session names to be published",
           "missing-name control produced an unexpected rejection: " + missing_error);
    expect(std::chrono::steady_clock::now() - missing_start < 3s,
           "missing-name timeout control exceeded its bound");

    const auto short_name = unique_name();
    const auto short_ready = marker_path(short_name);
    const auto short_release = std::filesystem::temp_directory_path() /
                               (short_name + ".release");
    std::string short_layout_error;
    std::filesystem::remove(short_ready);
    std::filesystem::remove(short_release);
    try {
        {
            Child short_mapping(peer, {"short-layout", short_name,
                                       short_ready.string(), short_release.string()});
            wait_marker(short_ready);
            try {
                auto invalid = Session::open(short_name);
                (void)invalid;
            } catch (const std::runtime_error& error) {
                short_layout_error = error.what();
            }
            write_text_file(short_release, "release\n");
            wait_success(short_mapping, 3s, "short-mapping injector");
        }
    } catch (...) {
        std::filesystem::remove(short_ready);
        std::filesystem::remove(short_release);
        (void)Session::remove(short_name);
        throw;
    }
    const bool short_removed = Session::remove(short_name);
    std::filesystem::remove(short_ready);
    std::filesystem::remove(short_release);
    expect(short_layout_error == "shared-memory object is smaller than its declared layout",
           "short-layout mapping produced an unexpected rejection: " + short_layout_error);
    expect(short_removed, "cleanup after short-mapping test failed");

    const auto index_name = unique_name();
    {
        auto session = Session::create(index_name, {2, 4});
        Child corrupter(peer, {"corrupt-index", index_name});
        wait_success(corrupter, 3s, "ring-index injector");
        expect(session.send(nullptr, 0, 100ms) == Status::invalid_frame,
               "out-of-range ring index was not rejected");
        expect(session.receive(100ms).status == Status::peer_failed,
               "session was not failed after a corrupt index");
    }
    expect(Session::remove(index_name), "cleanup after ring-index test failed");

    bool limits_rejected = false;
    try {
        auto impossible = Session::create(unique_name(), {4096, 1024U * 1024U});
        (void)impossible;
    } catch (const std::invalid_argument&) {
        limits_rejected = true;
    }
    expect(limits_rejected, "oversized capacity/payload product was not rejected");
}

void test_duplex_directional_channels(const std::string& peer) {
    const auto a_to_b = unique_name();
    const auto b_to_a = unique_name();
    {
        auto first = Session::create(a_to_b, {4, 32});
        auto second = Session::create(b_to_a, {4, 32});
        Child endpoint_a(peer, {"duplex", a_to_b, b_to_a, "A-", "B-", "100"});
        Child endpoint_b(peer, {"duplex", b_to_a, a_to_b, "B-", "A-", "100"});
        wait_success(endpoint_a, 12s, "duplex endpoint A");
        wait_success(endpoint_b, 12s, "duplex endpoint B");
    }
    expect(Session::remove(a_to_b), "cleanup of A-to-B direction failed");
    expect(Session::remove(b_to_a), "cleanup of B-to-A direction failed");
}

void test_duplex_cli_teardown(const std::string&, const std::string& demo) {
    const auto failed_outgoing = unique_name();
    const auto failed_incoming = unique_name();
    const auto failed_input = std::filesystem::temp_directory_path() /
                              (failed_outgoing + ".input");
    try {
        {
            auto outgoing = Session::create(failed_outgoing, {2, 4});
            auto incoming = Session::create(failed_incoming, {2, 4});
            write_text_file(failed_input, "12345\n");
            Child failing(demo, {"duplex", failed_outgoing, failed_incoming},
                          false, failed_input);
            const auto start = std::chrono::steady_clock::now();
            int exit_code = -1;
            expect(failing.wait_for(3s, exit_code),
                   "duplex CLI hung after an oversized outbound message with an idle open input ring");
            expect(std::chrono::steady_clock::now() - start < 3s,
                   "duplex CLI send-error teardown exceeded its 3-second bound");
            expect(exit_code != 0, "duplex CLI returned success after an oversized outbound message");
        }
        std::filesystem::remove(failed_input);
        expect(Session::remove(failed_outgoing), "cleanup of failed duplex outgoing session failed");
        expect(Session::remove(failed_incoming), "cleanup of failed duplex incoming session failed");
    } catch (...) {
        std::filesystem::remove(failed_input);
        (void)Session::remove(failed_outgoing);
        (void)Session::remove(failed_incoming);
        throw;
    }

    const auto clean_outgoing = unique_name();
    const auto clean_incoming = unique_name();
    const auto clean_input = std::filesystem::temp_directory_path() /
                             (clean_outgoing + ".input");
    try {
        {
            auto outgoing = Session::create(clean_outgoing, {2, 4});
            auto incoming = Session::create(clean_incoming, {2, 4});
            const char reply[] = "ack";
            expect(incoming.send(reply, sizeof(reply) - 1, 100ms) == Status::ok,
                   "clean duplex control could not queue its inbound message");
            expect(incoming.close_writer(100ms) == Status::ok,
                   "clean duplex control could not close its inbound writer");
            write_text_file(clean_input, "ping\n");
            Child clean(demo, {"duplex", clean_outgoing, clean_incoming},
                        false, clean_input);
            wait_success(clean, 3s, "duplex CLI clean-close control");
        }
        std::filesystem::remove(clean_input);
        expect(Session::remove(clean_outgoing), "cleanup of clean duplex outgoing session failed");
        expect(Session::remove(clean_incoming), "cleanup of clean duplex incoming session failed");
    } catch (...) {
        std::filesystem::remove(clean_input);
        (void)Session::remove(clean_outgoing);
        (void)Session::remove(clean_incoming);
        throw;
    }

    const auto flood_outgoing = unique_name();
    const auto flood_incoming = unique_name();
    const auto flood_input = std::filesystem::temp_directory_path() /
                             (flood_outgoing + ".input");
    constexpr std::uint32_t flood_capacity = 64;
    std::atomic<bool> keep_flooding{true};
    std::atomic<std::uint32_t> successful_flood_sends{0};
    std::thread flooder;
    bool sustained_input_seen_while_alive = false;
    bool flood_child_exited = false;
    std::chrono::milliseconds flood_child_runtime{0};
    int flood_exit_code = -1;
    try {
        {
            auto outgoing = Session::create(flood_outgoing, {2, 4});
            auto incoming = Session::create(flood_incoming, {flood_capacity, 4});
            write_text_file(flood_input, "one\ntwo\ntri\n");
            const auto child_start = std::chrono::steady_clock::now();
            Child failing(demo, {"duplex", flood_outgoing, flood_incoming},
                          false, flood_input);
            try {
                flooder = std::thread([&] {
                    const std::uint8_t message = 1;
                    while (keep_flooding.load()) {
                        const auto status = incoming.send(&message, sizeof(message), 100ms);
                        if (status == Status::ok) {
                            successful_flood_sends.fetch_add(1);
                        } else if (status != Status::timeout) {
                            return;
                        }
                    }
                });

                const auto child_deadline = child_start + 8s;
                while (std::chrono::steady_clock::now() < child_deadline) {
                    if (failing.wait_for(10ms, flood_exit_code)) {
                        flood_child_exited = true;
                        break;
                    }
                    if (successful_flood_sends.load() >= flood_capacity * 2) {
                        sustained_input_seen_while_alive = true;
                    }
                }
                flood_child_runtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - child_start);
                if (!flood_child_exited) failing.terminate();
            } catch (...) {
                keep_flooding.store(false);
                if (flooder.joinable()) flooder.join();
                throw;
            }
            keep_flooding.store(false);
            flooder.join();
        }
        std::filesystem::remove(flood_input);
        expect(Session::remove(flood_outgoing), "cleanup of flood duplex outgoing session failed");
        expect(Session::remove(flood_incoming), "cleanup of flood duplex incoming session failed");
    } catch (...) {
        keep_flooding.store(false);
        if (flooder.joinable()) flooder.join();
        std::filesystem::remove(flood_input);
        (void)Session::remove(flood_outgoing);
        (void)Session::remove(flood_incoming);
        throw;
    }
    expect(sustained_input_seen_while_alive,
           "continuous-input duplex regression did not sustain inbound sends while the demo was alive");
    expect(flood_child_runtime >= 4s,
           "continuous-input duplex regression did not reach the outgoing backpressure timeout");
    expect(flood_child_exited,
           "duplex CLI hung after an outbound timeout while inbound messages kept arriving");
    expect(flood_exit_code != 0,
           "duplex CLI returned success after its outbound send timed out");
}

} // namespace

int main(int argc, char** argv) {
    struct TestCase {
        const char* id;
        const char* description;
        void (*run)(const std::string&);
        void (*run_with_demo)(const std::string&, const std::string&) = nullptr;
    };
    const std::vector<TestCase> tests = {
        {"fifo", "FIFO ordering, wraparound, and shutdown", test_fifo_wraparound_and_shutdown},
        {"empty", "empty receive timeout", test_empty_timeout},
        {"full", "full backpressure and idle peer death", test_full_timeout_and_idle_peer_death},
        {"dead-lock", "dead synchronization owner and recreate policy", test_dead_lock_owner_requires_recreate},
        {"initialization", "creator/opener initialization gate", test_initialization_gate},
        {"roles", "duplicate producer/consumer role rejection", test_role_ownership},
        {"validation", "payload and hostile protocol validation", test_size_and_frame_validation},
        {"duplex", "duplex independent directions", test_duplex_directional_channels},
        {"duplex-cli", "duplex CLI error teardown under idle/flood input and clean close", nullptr,
         test_duplex_cli_teardown},
    };

    if (argc == 5 && std::string(argv[1]) == "--case") {
        const std::string id = argv[2];
        const auto found = std::find_if(tests.begin(), tests.end(), [&](const TestCase& test) {
            return id == test.id;
        });
        if (found == tests.end()) return 2;
        try {
            if (found->run != nullptr) found->run(argv[3]);
            else if (found->run_with_demo != nullptr) found->run_with_demo(argv[3], argv[4]);
            else return 2;
            return 0;
        } catch (const std::exception& error) {
            std::cerr << "worker " << found->id << ": " << error.what() << '\n';
            return 1;
        }
    }
    if (argc != 3) {
        std::cerr << "usage: shm_ring_tests <test-peer-executable> <demo-executable>\n";
        return 2;
    }
    const std::string peer = argv[1];
    const std::string demo = argv[2];
    const auto executable = std::filesystem::absolute(argv[0]).string();
    int failures = 0;
    for (const auto& test : tests) {
        try {
            // Each scenario is isolated in a worker process. The parent owns
            // an independent watchdog; POSIX process groups / Windows jobs
            // ensure a timed-out worker cannot strand its endpoint children.
            Child worker(executable, {"--case", test.id, peer, demo}, true);
            wait_success(worker, 10s, test.description);
            std::cout << "PASS " << test.description << '\n';
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "FAIL " << test.description << ": " << error.what() << '\n';
        }
    }
    std::cout << (tests.size() - static_cast<std::size_t>(failures)) << "/" << tests.size()
              << " correctness scenarios passed\n";
    return failures == 0 ? 0 : 1;
}
