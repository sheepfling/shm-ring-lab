# Verification backlog

The private HTTPS origin exists but has no branch refs; no local changes have been pushed. No CI run or Umbra integration is part of this project. Record actual compiler/OS and exact command output when advancing a phase.

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

## Phase 1 — Linux target validation

- [x] Added `docker/linux/Dockerfile`, `.dockerignore`, `scripts/docker-linux-proof.sh`, and a bounded separate-process proof. When run, it builds Debian Bookworm with GCC/CMake, repeats all eight CTest scenarios three times, then transfers 5,000 frames between independent producer/consumer processes and checks count, bytes, and SHA-256. The entrypoint prints the exact container OS/architecture and compiler/CMake versions.
- [x] 2026-10-11: Docker Desktop 29.8.0 ran the proof on Linux/aarch64. Container: Debian GNU/Linux 12 (Bookworm), LinuxKit kernel 7.0.12, GCC 12.2.0, CMake 3.25.1. Image build passed; CTest passed 3 consecutive repetitions (1/1 each, 15.04 s total; all 8 scenarios per run); the separate-process proof passed 5,000 messages / 165,000 bytes with SHA-256 `29ac1b5138c2db00f5a2710813afc39a188e77496b56b9ce19c60d2c6db65507`. Command: `bash scripts/docker-linux-proof.sh`. This is Linux container evidence, not a native Linux host run.
- [ ] Build with a named compiler/toolchain and CMake version; record distro, kernel, architecture, libc, compiler, and exact commands.
- [ ] Run all CTest correctness scenarios with process deadlines; repeat under a second compiler (GCC/Clang) where available.
- [ ] Verify `shm_open`, `sem_open`, permissions, cleanup after normal exit, and bounded timeout after the `crash-lock` helper exits while holding the semaphore.
- [ ] Run sanitizers for process-local memory errors where supported; do not interpret them as crash-recovery proof.

## Phase 2 — Windows target validation

- [x] Added the CMake 3.21+ `windows-msvc-x64` preset and [PB-MSI proof checklist](windows-proof-checklist.md), including CTest, independent producer/consumer integrity/count/SHA-256, and environment capture commands.
- [ ] Build with MSVC and CMake on a supported Windows version; record SDK/compiler/architecture and exact commands.
- [ ] Run all process tests, including `WAIT_ABANDONED`, `Local\` object naming, process termination, duplex, cleanup-after-final-handle, and timeout behavior.
- [ ] Repeat from two terminals under the same user logon session and confirm a second user/session cannot accidentally join the object.
- [ ] Check x64 first; add ARM64 only if it is a stated target.

## Phase 3 — protocol hardening and bounded next milestone

- [ ] Decide whether to add a stable serialized header ABI across compiler versions or keep the current same-build transient ABI contract.
- [ ] Add an explicit session-owner/lease or supervisor policy only if automatic peer-death detection is required; do not infer liveness from a PID alone.
- [ ] Add randomized wraparound and load tests, preserving fixed deadlines and separating them from gating correctness tests.
- [ ] Consider optional event-based wakeups only after measuring the polling baseline; preserve the named-lock serialization and deadline contract.
