# shm-ring-lab

A small C++17 test bed for bounded, copied messages between independent processes. The first milestone is one-producer/one-consumer (SPSC) per direction, with a separate reverse ring for duplex traffic. It favors explicit limits and bounded waits over lock-free or zero-copy behavior.

## Build and run

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release --parallel
ctest --test-dir build -C Release --output-on-failure
```

For a multi-configuration Windows generator, executables are under `build/Release/` (for example, `build\Release\shm_ring_demo.exe`).

Create one direction, then run a consumer and producer in separate terminals:

```sh
build/shm_ring_demo create demo-ab 8 1024
build/shm_ring_demo consume demo-ab
printf 'one\ntwo\nthree\n' | build/shm_ring_demo produce demo-ab
build/shm_ring_demo cleanup demo-ab
```

The producer closes its writer at end-of-input. The consumer drains queued messages and exits on end-of-stream. For duplex traffic, create two names and run the endpoint command in each process:

```sh
build/shm_ring_demo create a-to-b
build/shm_ring_demo create b-to-a
# Process A: build/shm_ring_demo duplex a-to-b b-to-a
# Process B: build/shm_ring_demo duplex b-to-a a-to-b
```

Each direction has its own bounded ring and synchronization object. End each process's input with EOF to close that process's outgoing direction. Remove both names after the processes stop.

## Contract

- Windows and Linux are intended targets; macOS uses the POSIX backend for development and secondary support. This checkout was built and exercised only on macOS; see [the verification backlog](docs/verification-backlog.md).
- The app API is in [`include/shm_ring_lab/session.hpp`](include/shm_ring_lab/session.hpp). `src/platform/` holds explicit OS backends. No third-party package is required.
- V1 uses OS named synchronization (POSIX named semaphore or Win32 named mutex), not standard C++ atomics or an assumed process-shared `std::mutex`.
- A session is transient shared memory, not durable storage. It has a fixed slot count and fixed maximum payload size. Each operation copies bytes while holding the process-shared lock.
- Session names are 1–24 ASCII letters, digits, `_` or `-`; names are case-normalized. Create a fresh unique name for each session and publish it to peers only after `create` returns.
- Ring order is FIFO. Full and empty operations wait only until their supplied deadline. Oversized outgoing messages fail immediately; malformed shared frames fail the session. `close_writer()` produces a drain-then-closed end-of-stream.
- A crashed peer is not automatically monitored. A peer that dies while idle can leave a full ring or a reader waiting until its deadline. POSIX named semaphores cannot recover a permit abandoned by a dead process; operations time out and the caller must discard that session and create a new name. Windows reports an abandoned mutex and this implementation marks the session failed. No abandoned state is repaired.
- POSIX names persist until `Session::remove`; call it after all peers stop. Windows mapping and mutex names disappear when the last handle closes.

The protocol layout and synchronization limits are described in [`docs/protocol-v1.md`](docs/protocol-v1.md); the milestones for native target validation are in [`docs/verification-backlog.md`](docs/verification-backlog.md).

## Non-gating benchmark

Correctness tests are separate from benchmarks. Build benchmark helpers only when wanted:

```sh
cmake -S . -B build-bench -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-bench --target shm_ring_demo shm_ring_bench_peer --parallel
build-bench/shm_ring_demo create bench-run 64 4096
# In separate terminals, use the same count and payload size:
build-bench/shm_ring_bench_peer consume bench-run 100000 1024
build-bench/shm_ring_bench_peer produce bench-run 100000 1024
build-bench/shm_ring_demo cleanup bench-run
```

Benchmark output is intentionally not a CTest gate. Keep redirected output under the ignored `benchmark-results/` directory.
