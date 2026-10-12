#include "shm_ring_lab/session.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
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
    Child(const std::string& executable, const std::vector<std::string>& args) {
        std::wstring command = quote_argument(widen(executable));
        for (const auto& arg : args) command += L" " + quote_argument(widen(arg));
        std::vector<wchar_t> mutable_command(command.begin(), command.end());
        mutable_command.push_back(L'\0');
        STARTUPINFOW startup{};
        startup.cb = sizeof(startup);
        if (!::CreateProcessW(nullptr, mutable_command.data(), nullptr, nullptr, FALSE,
                              CREATE_NO_WINDOW, nullptr, nullptr, &startup, &process_)) {
            throw std::runtime_error("CreateProcessW failed (" + std::to_string(::GetLastError()) + ")");
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
            (void)::TerminateProcess(process_.hProcess, 137);
            (void)::WaitForSingleObject(process_.hProcess, 5000);
            finished_ = true;
        }
    }

private:
    void close_handles() noexcept {
        if (process_.hProcess != nullptr) ::CloseHandle(process_.hProcess);
        process_.hProcess = nullptr;
    }
    PROCESS_INFORMATION process_{};
    bool finished_ = false;
};

#else

class Child {
public:
    Child(const std::string& executable, const std::vector<std::string>& args) {
        pid_ = ::fork();
        if (pid_ < 0) throw std::runtime_error("fork failed");
        if (pid_ == 0) {
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
            (void)::kill(pid_, SIGKILL);
            int status = 0;
            while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {}
            finished_ = true;
        }
    }

private:
    pid_t pid_ = -1;
    bool finished_ = false;
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
        auto opened = Session::open(name);
        const std::uint8_t value = 1;
        const auto start = std::chrono::steady_clock::now();
        const auto status = opened.send(&value, 1, 200ms);
        expect(status == Status::timeout || status == Status::peer_failed,
               "abandoned synchronization state was treated as usable");
        expect(std::chrono::steady_clock::now() - start < 1500ms,
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

void test_size_and_frame_validation(const std::string& peer) {
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

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: shm_ring_tests <test-peer-executable>\n";
        return 2;
    }
    const std::string peer = argv[1];
    const std::vector<std::pair<const char*, void(*)(const std::string&)>> tests = {
        {"FIFO ordering, wraparound, and shutdown", test_fifo_wraparound_and_shutdown},
        {"empty receive timeout", test_empty_timeout},
        {"full backpressure and idle peer death", test_full_timeout_and_idle_peer_death},
        {"dead synchronization owner and recreate policy", test_dead_lock_owner_requires_recreate},
        {"payload and protocol validation", test_size_and_frame_validation},
        {"duplex independent directions", test_duplex_directional_channels},
    };
    int failures = 0;
    for (const auto& test : tests) {
        try {
            test.second(peer);
            std::cout << "PASS " << test.first << '\n';
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "FAIL " << test.first << ": " << error.what() << '\n';
        }
    }
    std::cout << (tests.size() - static_cast<std::size_t>(failures)) << "/" << tests.size()
              << " correctness scenarios passed\n";
    return failures == 0 ? 0 : 1;
}
