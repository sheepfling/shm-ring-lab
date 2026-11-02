# Java SPSC process-ring prototype

This Java 17 branch now contains two independent file-backed SPSC implementations:

- `FileRing` is the initial positional-I/O prototype. It reads and writes slots through `FileChannel` while holding a byte-range `FileLock`.
- `MappedFileRing` maps the session file with `FileChannel.map(READ_WRITE)` and copies bounded messages into fixed-size mapped slots. A byte-range `FileLock` serializes each operation across JVMs; a process-local `ReentrantLock` serializes channels within one JVM. Payload bytes and sequence/header updates are explicitly forced before the operation reports success. Empty/full waits poll with a 1 ms interval and obey the caller's deadline.

The mapped implementation has its own 64-byte little-endian header (`JMR1`, version 1), fixed slot sizes, monotonically increasing read/write sequences, and writer-closed flag. It supports one producer and one consumer. It is a correctness-first mapped-file backend: it still copies payloads, serializes operations with OS file locks, and forces updates. No zero-copy or latency claim is made.

## Build and test

Requires JDK 17 or newer:

```sh
bash scripts/test.sh
```

The script compiles with `javac --release 17 -Xlint:all -Werror`, runs the original `FileRing` tests, then runs `MappedFileRing` tests. The mapped tests cover FIFO/wraparound, full and empty deadlines, invalid/oversized payloads, writer close/end-of-stream, cross-process file-lock contention, a 500-message transfer between separate producer and consumer JVMs, and cleanup after all mapped-file users exit. Each child has a bounded wait and is forcibly terminated on timeout.

The mapped test harness can also be run by itself after compilation:

```sh
java -cp build/classes lab.shmring.MappedMain selftest
```

## IPC and lifecycle contract

These sessions are ordinary same-host files opened by path and memory-mapped by Java. They are not POSIX `shm_open` objects or Windows named `CreateFileMapping` objects; they do not establish a common namespace or permissions model with the C++ implementation. The `JMR1` layout and locking protocol are Java-specific and are not C++ wire/ABI compatible.

Create the file once with `MappedFileRing.create`, wait until initialization completes, then have the peer open that same path with `MappedFileRing.open`. The creator owns session naming and eventual deletion. Delete only after every peer has closed the session and exited. Java 17 has no supported portable deterministic unmap API; closing a ring closes its channel and drops its map reference, but on Windows the OS mapping may remain until garbage collection/process exit. The test deletes files only after the hosting JVMs have exited.

The OS file lock is the interprocess serialization primitive. The implementation does not use shared `Atomic*`, `synchronized`, or `ReentrantLock` objects as cross-process synchronization. Each lock acquisition is bounded by the operation deadline. `MappedByteBuffer.force()` is used to publish slot bytes before advancing the write sequence and to persist sequence/header changes; this is not a transaction or crash-recovery guarantee. If a process dies during an operation, discard the session and create a fresh file. No abandoned-operation repair is claimed.

Files created on POSIX hosts request owner read/write permissions (`0600`). On platforms without POSIX file attributes, the file uses the platform's inherited default ACL. Choose a private directory with appropriate access control. Treat these session files as transient IPC state, not durable storage.

## Verification evidence

On 2026-11-02, the original and mapped test suites passed in the Eclipse Temurin 17 container on LinuxKit/aarch64 with:

```sh
docker run --rm -v "$PWD":/src -w /src eclipse-temurin:17-jdk bash scripts/test.sh
```

The passing output included the original two-process ring suite and `PASS mapped shared-file transfer across three JVMs and post-exit cleanup`. The host has no locally installed Java runtime. The earlier file-backed-only commit `9f320df` passed the existing hosted Ubuntu, macOS, and Windows workflow (run `41543791667`); that run does not validate the new mapped backend. The mapped implementation still needs a fresh hosted CI run on all three operating systems before platform support is claimed.

## Scope and limits

- SPSC only; no MPMC, durable queue, distributed shared memory, JNI/JNA, or named OS shared-memory backend.
- The path must refer to the same file for every JVM. Network filesystems and cross-host use are unsupported.
- `force()` and file locking prioritize clear ordering and correctness over low latency; benchmark only after the semantics and OS behavior are validated.
- No C++ interoperability is claimed. Agree a serialized ABI and cross-language synchronization protocol separately before attempting it.
- An abnormal peer exit can leave an incomplete operation. Fail the session and recreate it under a new path; do not assume recoverability.
