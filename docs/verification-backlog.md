# Verification backlog

This backlog combines dated local results with the current review state. Draft PR #1 publishes the base C++ project and Python tutorial on `feature/initial-work`; the October 18 paired IPC benchmark remains separate local work and is not included in the PR. GitHub Actions now builds and runs CTest on hosted Ubuntu, macOS, and Windows runners for pull requests.

The October 30 check found no PR reviews, status contexts, or PR-triggered workflow runs at that time. Do not treat dated local runs below as CI results. No Umbra integration is part of this project. Record actual compiler/OS and exact command output when advancing a phase.

## Phase 0 — local macOS development

- [x] Implement POSIX shared memory and named semaphore backend.
- [x] Add bounded process-level ordering, wraparound, empty/full, shutdown, malformed-frame, protocol-version/layout, creator/opener race, role ownership, duplex, and peer-death scenarios. Each case runs in a separate worker with a 10-second parent watchdog; POSIX process groups and Windows Job Objects terminate its endpoints on timeout. CTest adds a 90-second outer deadline.
- [x] Direct Clang C++17 builds on macOS 26.6.2 arm64 with Apple Clang 21.0.0: demo, integration runner, process peer, and optional benchmark helper compiled with `-Wall -Wextra -Wpedantic`.
- [x] CMake 4.4.4 Release configure/build passed. Commands: `cmake -S . -B /tmp/shm-ring-lab-build/cmake -DCMAKE_BUILD_TYPE=Release`; `cmake --build /tmp/shm-ring-lab-build/cmake --config Release --parallel`; `ctest --test-dir /tmp/shm-ring-lab-build/cmake -C Release --repeat until-fail:3 --output-on-failure`.
- [x] After the initialization, mapping-validation, deadline, and process-watchdog review fixes, CMake Release rebuild passed and CTest passed 3 consecutive repetitions (1/1 each, 15.42 s total); each repetition ran all 8 bounded scenarios. The `BUILD_BENCHMARKS=ON` configuration also compiled `shm_ring_demo` and `shm_ring_bench_peer`; the benchmark was not run.
- [x] 2026-10-11: added the `host` CLI command so the session owner can keep OS handles open during the manual showcase, including on Windows. The canonical checkout built with macOS 26.6.2 arm64 / Apple Clang 21.0.0 / CMake 4.4.4; the latest CTest run passed 3 consecutive repetitions (1/1 each, 15.41 s total; all 8 scenarios per run). Commands: `cmake -S . -B /tmp/shm-ring-lab-canonical-build -DCMAKE_BUILD_TYPE=Release`; `cmake --build /tmp/shm-ring-lab-canonical-build --parallel`; `ctest --test-dir /tmp/shm-ring-lab-canonical-build --repeat until-fail:3 --output-on-failure`.
- [x] 2026-10-11: exercised the README file-based host, separate producer, and separate consumer flow from the canonical checkout on macOS. Three input lines were received byte-for-byte and the output line count was 3. The README contains exact POSIX-shell and Windows Command Prompt walkthroughs with content and count checks.
- [x] 2026-10-11: ran the new 5,000-frame process-proof harness against the native macOS demo binary: 5,000 messages, 165,000 bytes, matching SHA-256 `29ac1b5138c2db00f5a2710813afc39a188e77496b56b9ce19c60d2c6db65507`. This validates the harness on macOS only, not Linux.
- [x] Separate producer/consumer CLI smoke passed with `hello` and `world`; two-process duplex CLI smoke exchanged one message each way over separate sessions.
- [ ] Repeat tests on Intel macOS if that architecture is part of the support promise.
- [x] 2026-11-02: GitHub Actions macOS hosted runner passed configure/build and CTest (1/1, all 9 process scenarios) on PR #1 commit `818c708`.
- [x] 2026-11-02: Apple Silicon native Release configure/build passed with macOS 26.6.2, Apple Clang 21.0.0, and CMake 4.4.4. CTest passed 1/1 registered test, covering all 9 process scenarios (13.48 s). Initial sandboxed test setup hit `shm_open` `errno 1`; the same bounded CTest command passed after per-command approval. Exact commands: `cmake -S . -B /tmp/shm-ring-lab-final-build -DCMAKE_BUILD_TYPE=Release`; `cmake --build /tmp/shm-ring-lab-final-build --config Release --parallel 2`; `ctest --test-dir /tmp/shm-ring-lab-final-build --build-config Release --output-on-failure`.

## Phase 1 — Linux target validation

- [x] Added `docker/linux/Dockerfile`, `.dockerignore`, `scripts/docker-linux-proof.sh`, and a bounded separate-process proof. When run, it builds Debian Bookworm with GCC/CMake, repeats all eight CTest scenarios three times, then transfers 5,000 frames between independent producer/consumer processes and checks count, bytes, and SHA-256. The entrypoint prints the exact container OS/architecture and compiler/CMake versions.
- [x] 2026-10-11: Docker Desktop 29.8.0 ran the proof on Linux/aarch64. Container: Debian GNU/Linux 12 (Bookworm), LinuxKit kernel 7.0.12, GCC 12.2.0, CMake 3.25.1. Image build passed; CTest passed 3 consecutive repetitions (1/1 each, 15.04 s total; all 8 scenarios per run); the separate-process proof passed 5,000 messages / 165,000 bytes with SHA-256 `29ac1b5138c2db00f5a2710813afc39a188e77496b56b9ce19c60d2c6db65507`. Command: `bash scripts/docker-linux-proof.sh`. This is Linux container evidence, not a native Linux host run.
- [ ] Build with a named compiler/toolchain and CMake version; record distro, kernel, architecture, libc, compiler, and exact commands.
- [ ] Run all CTest correctness scenarios with process deadlines; repeat under a second compiler (GCC/Clang) where available.
- [ ] Verify `shm_open`, `sem_open`, permissions, cleanup after normal exit, and bounded timeout after the `crash-lock` helper exits while holding the semaphore.
- [ ] Run sanitizers for process-local memory errors where supported; do not interpret them as crash-recovery proof.
- [x] 2026-11-02: GitHub Actions `ubuntu-latest` passed configure/build and all 9 CTest process scenarios on PR #1 commit `818c708`. This is hosted native Linux evidence; the separate aarch64 Docker run remains container evidence.
- [x] 2026-11-02: Docker Desktop 29.8.0 ran the updated proof in Debian 12 / LinuxKit 7.0.12 / aarch64 with GCC 12.2.0 and CMake 3.25.1. Build passed; all 9 CTest scenarios passed 3 consecutive repetitions (36.47 s total); the separate-process proof passed 5,000 messages / 165,000 bytes with SHA-256 `29ac1b5138c2db00f5a2710813afc39a188e77496b56b9ce19c60d2c6db65507`. Command: `bash scripts/docker-linux-proof.sh`. This is container evidence, not native Linux host validation.

## Phase 2 — Windows target validation

- [x] Added the CMake 3.21+ `windows-msvc-x64` preset and [PB-MSI proof checklist](windows-proof-checklist.md), including CTest, independent producer/consumer integrity/count/SHA-256, and environment capture commands.
- [ ] Build with MSVC and CMake on a supported Windows version; record SDK/compiler/architecture and exact commands.
- [ ] Run all process tests, including `WAIT_ABANDONED`, `Local\` object naming, process termination, duplex, cleanup-after-final-handle, and timeout behavior.
- [ ] Repeat from two terminals under the same user logon session and confirm a second user/session cannot accidentally join the object.
- [ ] Check x64 first; add ARM64 only if it is a stated target.
- [x] 2026-11-02: GitHub Actions Windows Server 2025 hosted runner, Visual Studio 2026 generator, MSVC 19.51.36260.0, x64; configure/build passed and CTest passed 1/1 (all 9 process scenarios, 10.84 s) on PR #1 commit `818c708`. This is actual hosted Windows execution; the checklist's multi-terminal/session-identity checks remain outstanding.
- [ ] Complete the PB-MSI same-session and different-session checklist. Hosted CI does not cover this identity/security boundary.

## Current cleanup note

- [x] 2026-11-02: POSIX test-child teardown now sends `SIGKILL` and polls `waitpid(WNOHANG)` for at most 5 seconds instead of waiting indefinitely. If the kernel does not reap the child by then, the test prints a diagnostic and returns control so the enclosing worker can exit and the OS can finish reaping. Current macOS and Docker Linux correctness suites passed with this change.

## Phase 3 — protocol hardening and bounded next milestone

- [ ] Decide whether to add a stable serialized header ABI across compiler versions or keep the current same-build transient ABI contract.
- [ ] Add an explicit session-owner/lease or supervisor policy only if automatic peer-death detection is required; do not infer liveness from a PID alone.
- [ ] Add randomized wraparound and load tests, preserving fixed deadlines and separating them from gating correctness tests.
- [ ] Consider optional event-based wakeups only after measuring the polling baseline; preserve the named-lock serialization and deadline contract.
