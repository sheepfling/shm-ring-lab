# Verification backlog

No remote, push, CI run, or Umbra integration is part of this project. Record actual compiler/OS and exact command output when advancing a phase.

## Phase 0 — local macOS development

- [x] Implement POSIX shared memory and named semaphore backend.
- [x] Add bounded process-level ordering, wraparound, empty/full, shutdown, malformed-frame, protocol-version, duplex, and peer-death scenarios.
- [x] Direct Clang C++17 builds on macOS 26.6.2 arm64 with Apple Clang 21.0.0: demo, integration runner, process peer, and optional benchmark helper all compiled with `-Wall -Wextra -Wpedantic`.
- [x] Process test runner: 6/6 scenarios passed (FIFO/order/wraparound/shutdown; empty timeout; full timeout and idle peer death; dead lock owner/recreate; payload/protocol validation; duplex directions). Separate producer/consumer CLI smoke passed with `hello` and `world`; two-process duplex CLI smoke exchanged one message each way over separate sessions.
- [ ] Run and record the CMake/CTest workflow on a Mac with CMake installed. Current machine has no `cmake`; no package was installed. `brew install cmake` is the exact setup command needed before this check.
- [ ] Repeat tests on Intel macOS if that architecture is part of the support promise.

## Phase 1 — Linux target validation

- [ ] Build with a named compiler/toolchain and CMake version; record distro, kernel, architecture, libc, compiler, and exact commands.
- [ ] Run all CTest correctness scenarios with process deadlines; repeat under a second compiler (GCC/Clang) where available.
- [ ] Verify `shm_open`, `sem_open`, permissions, cleanup after normal exit, and bounded timeout after the `crash-lock` helper exits while holding the semaphore.
- [ ] Run sanitizers for process-local memory errors where supported; do not interpret them as crash-recovery proof.

## Phase 2 — Windows target validation

- [ ] Build with MSVC and CMake on a supported Windows version; record SDK/compiler/architecture and exact commands.
- [ ] Run all process tests, including `WAIT_ABANDONED`, `Local\` object naming, process termination, duplex, cleanup-after-final-handle, and timeout behavior.
- [ ] Repeat from two terminals under the same user logon session and confirm a second user/session cannot accidentally join the object.
- [ ] Check x64 first; add ARM64 only if it is a stated target.

## Phase 3 — protocol hardening and bounded next milestone

- [ ] Decide whether to add a stable serialized header ABI across compiler versions or keep the current same-build transient ABI contract.
- [ ] Add an explicit session-owner/lease or supervisor policy only if automatic peer-death detection is required; do not infer liveness from a PID alone.
- [ ] Add randomized wraparound and load tests, preserving fixed deadlines and separating them from gating correctness tests.
- [ ] Consider optional event-based wakeups only after measuring the polling baseline; preserve the named-lock serialization and deadline contract.
