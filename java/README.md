# Java SPSC process-ring prototype

This Java 17 branch now contains two independent file-backed SPSC implementations:

- `FileRing` is the initial positional-I/O prototype. It reads and writes slots through `FileChannel` while holding a byte-range `FileLock`.
- `MappedFileRing` maps the session file with `FileChannel.map(READ_WRITE)` and copies bounded messages into fixed-size mapped slots. A byte-range `FileLock` serializes each operation across JVMs; a library-wide `ReentrantLock` serializes mapped-ring file-lock critical sections within one class loader, including path aliases. It does not call `load()` or `force()` on each operation. Empty/full waits poll with a 1 ms interval and obey the caller's deadline.

The mapped implementation has its own 64-byte little-endian header (`JMR1`, version 1), fixed slot sizes, monotonically increasing read/write sequences, and writer-closed flag. It supports one producer and one consumer. It is a correctness-first mapped-file backend: it still copies payloads and serializes operations with OS file locks. It does not force transient IPC writes to durable storage. No zero-copy or latency claim is made.

## Build and test

Requires JDK 17 or newer:

```sh
bash scripts/test.sh
```

The script compiles with `javac --release 17 -Xlint:all -Werror`, runs the original `FileRing` tests, then runs `MappedFileRing` tests. The mapped tests cover FIFO/wraparound, full and empty deadlines, invalid/oversized payloads, writer close/end-of-stream, cross-process file-lock contention, a 500-message transfer between separate producer and consumer JVMs, an idle poll on a sparse 32 MiB mapping, and cleanup after all mapped-file users exit. A hard-link alias test opens two handles to the same file, races close of one handle against a 16 MiB write through the other, verifies close waits until the write finishes, and checks the exact payload. Each child has a bounded wait and is forcibly terminated on timeout.

The mapped test harness can also be run by itself after compilation:

```sh
java -cp build/classes lab.shmring.MappedMain selftest
```

## IPC and lifecycle contract

These sessions are ordinary same-host files opened by path and memory-mapped by Java. They are not POSIX `shm_open` objects or Windows named `CreateFileMapping` objects; they do not establish a common namespace or permissions model with the C++ implementation. The `JMR1` layout and locking protocol are Java-specific and are not C++ wire/ABI compatible.

Create the file once with `MappedFileRing.create`, wait until initialization completes, then have the peer open that same path with `MappedFileRing.open`. The creator owns session naming and eventual deletion. A library-wide lifecycle gate makes `close()` wait for active mapped-ring operations before any mapped-ring handle closes, including handles opened through symlink or hard-link aliases; the critical-section gate does not depend on pathname identity. This coordination covers instances of this class loaded by one class loader. Do not mix direct `FileChannel` locks or a duplicate copy of this library in another class loader against the same session. Close then shuts only that JVM's handle and causes later operations through that instance to throw `IOException`; it does not close the shared writer session for peers. Use `closeWriter()` for end-of-stream. Delete only after every peer has closed the session and exited. Java 17 has no supported portable deterministic unmap API; closing a ring closes its channel and drops its map reference, but on Windows the OS mapping may remain until garbage collection/process exit. The test deletes files only after the hosting JVMs have exited.

The OS file lock is the cooperative interprocess serialization primitive. The implementation does not use shared `Atomic*`, `synchronized`, or `ReentrantLock` objects as cross-process synchronization. Each lock acquisition and full/empty wait is bounded by the operation deadline; page faults and memory access time are controlled by the OS and are not a hard real-time bound. Java does not specify a cross-JVM happens-before or visibility guarantee for ordinary mapped-buffer accesses: its `MappedByteBuffer` documentation says external changes and when they become visible are OS-dependent, and the `FileChannel` documentation likewise leaves consistency with concurrently running programs unspecified. `FileLock` is intended to map to native locks, but may be advisory; every participant must use this protocol. This implementation therefore relies on coherent shared mappings plus cooperative local-file locking on the tested OS/filesystem combinations, not a portable Java memory-model guarantee. CI exercises Ubuntu, macOS, and Windows runners, but does not certify arbitrary filesystems or network storage.

Neither `MappedByteBuffer.load()` nor `force()` is used as a visibility barrier. `load()` is only a best-effort request to page the mapping into physical memory and may incur page faults and I/O. `force()` writes modified mapped bytes to local storage, which this transient IPC session does not require; it is not a transaction or crash-recovery guarantee. If a process dies during an operation, discard the session and create a fresh file. No abandoned-operation repair is claimed.

Files created on POSIX hosts request owner read/write permissions (`0600`). On platforms without POSIX file attributes, the file uses the platform's inherited default ACL. Choose a private directory with appropriate access control. Treat these session files as transient IPC state, not durable storage.

## Verification evidence

On 2026-11-02, the original and mapped test suites passed in the Eclipse Temurin 17 container on LinuxKit/aarch64 with:

```sh
docker run --rm -v "$PWD":/src -w /src eclipse-temurin:17-jdk bash scripts/test.sh
```

The final local rerun passed the original two-process suite and the mapped transfer, 32 MiB sparse-map poll, hard-link alias close race during a 16 MiB write, and post-exit cleanup checks after removing all `load()`/`force()` calls from the mapped implementation. The host has no locally installed Java runtime. GitHub Actions run `41548404766` passed the updated suite on `ubuntu-latest`, `macos-latest`, and `windows-latest` for alias-safety commit `23263b2f7bef157130924f96e115c2c18fc565dc`. Earlier mapped implementation runs `41547387473` and `41547554304` preceded the alias change; file-backed-only run `41543791667` did not validate the mapped backend.

The synchronization limits above follow the JDK 17 contracts for [`MappedByteBuffer`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/MappedByteBuffer.html), [`FileChannel`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/channels/FileChannel.html), and [`FileLock`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/channels/FileLock.html). These APIs do not promise a portable cross-JVM happens-before edge for mapped accesses.

## Scope and limits

- SPSC only; no MPMC, durable queue, distributed shared memory, JNI/JNA, or named OS shared-memory backend.
- The path must refer to the same file for every JVM. Network filesystems and cross-host use are unsupported.
- File locking prioritizes clear cooperative ordering over low latency; benchmark only after the semantics and OS behavior are validated.
- No C++ interoperability is claimed. Agree a serialized ABI and cross-language synchronization protocol separately before attempting it.
- An abnormal peer exit can leave an incomplete operation. Fail the session and recreate it under a new path; do not assume recoverability.
