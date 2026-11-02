# Java file-backed SPSC ring prototype

This issue branch starts an independent Java 17 implementation of the lab's bounded SPSC process-message API. It uses a versioned file layout and `FileChannel` positional I/O with an exclusive byte-range `FileLock`; it does not use JNI/JNA or the C++ POSIX/Win32 session format. Peers must use the same file path and this Java implementation. Session files are transient IPC state, not durable storage.

Messages are copied into fixed-size slots. The API includes FIFO ordering, wraparound, full/empty deadlines, a one-way writer close, and a pointer-free fixed-width header. Every operation takes the file lock and checks the header before touching slots. This is a correctness-first file-backed IPC prototype, not an mmap/zero-copy backend.

## Run

Requires a JDK 17 or newer:

```sh
bash scripts/test.sh
```

The test creates a capacity-two ring to check full timeout, FIFO wraparound, and clean close, then transfers 500 messages between separately launched producer and consumer JVMs under a 15-second process deadline.

For a manual exchange, first create a ring and leave its host process running:

```sh
java -cp build/classes lab.shmring.Main host /tmp/demo.ring 16 256
```

In two other terminals run:

```sh
java -cp build/classes lab.shmring.Main consume /tmp/demo.ring 500
java -cp build/classes lab.shmring.Main produce /tmp/demo.ring 500
```

Run the consumer before the producer. Press Enter in the host terminal after both
peers exit, then remove the session file. Remove session files only after all
peers have closed them.

## Limits

- This prototype is not wire-compatible with the C++ transport and does not claim shared-memory mapping performance.
- Use one producer and one consumer. A local JVM lock handles overlapping `FileLock` attempts between channels in the same JVM; the OS file lock coordinates processes.
- A process crash releases its OS file lock, but a crash during an operation can leave partially updated state. Treat abnormal peer exit as a failed session and recreate a fresh file; no owner-death recovery is claimed.
- Run the correctness harness on Linux, macOS, and Windows before declaring support. CI coverage is included as a separate check, not as a replacement for review of the file-lock semantics.

## Initial test evidence

On 2026-11-02, `bash scripts/test.sh` passed in an official Eclipse Temurin
17.0.20.1 JDK container on LinuxKit 7.0.12 / aarch64. The test covered FIFO,
wraparound, full and empty deadlines, cross-process file-lock contention,
writer close, and a 500-message producer/consumer transfer between separate
JVMs. This is container evidence; native macOS, Linux-host, and Windows CI runs
remain pending.
