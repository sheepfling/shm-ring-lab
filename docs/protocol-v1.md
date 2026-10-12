# Protocol notes: V1

## Shared layout

Each named session maps one fixed-size region. The beginning contains a magic value (`SHMRING1`), protocol version, native header size, expected mapping size, slot capacity, maximum payload, ring indices/count, writer-closed and failed flags, and writer/reader process IDs. The rest contains `capacity` fixed-width slot headers followed by each slot's reserved payload bytes. Slots contain a 32-bit byte length and copied payload bytes. No pointer, `std::string`, vector, virtual object, or process-local handle is shared.

V1 validates the magic, version, header size, mapping size, limits, and ring indices before use. A version or layout mismatch is rejected; V1 does not migrate data. The structure uses fixed-width fields, but native structure padding is part of the transient ABI and checked with `header_size`. Both processes must use matching architecture/compiler ABI and this protocol implementation. A session is not a file format and is not meant to survive reboot, OS changes, or mixed 32/64-bit participants.

## Bounds and ownership

- Capacity is 2–4096 messages; each payload is 1–1,048,576 bytes; total mapping size is at most 64 MiB.
- A session has one writer process and one reader process. Role ownership is claimed on first use (or explicitly with `claim_writer` / `claim_reader`) and duplicate roles from another PID receive `role_in_use`. The application contract is one writer thread and one reader thread per direction.
- Shared-memory and synchronization object names are derived from a validated, case-normalized 1–24 character logical name. Create only once; peers open only after the creator has returned from `create`.
- The creator configures capacity and payload size. A writer copies into a free slot while holding the OS lock. A reader copies a complete slot into process-local memory before releasing the lock. No zero-copy lifetime or pointer ownership is implied.
- POSIX mappings are mode `0600`; Windows uses the current logon session's `Local\` namespace and default object security. Use peers under the same user/session identity.

## Ordering, pressure, and close

The ring is FIFO. A successful `send` publishes one complete frame; a successful `receive` returns one whole frame. Empty reads and full writes poll under a short lock, releasing it between attempts, and stop at the caller's steady-clock deadline. There is no unbounded wait API. A zero-byte message is valid. A null data pointer with nonzero length is invalid. An outgoing message above the configured maximum returns `too_large`; a stored length above the maximum marks the session failed and returns `invalid_frame`.

`close_writer` blocks subsequent writes and preserves already queued frames. The reader drains those frames and then receives `closed`. If a writer exits without closing, a waiting reader eventually gets `timeout`; the protocol has no implicit end-of-stream on process exit.

## Synchronization and peer death

- POSIX: `shm_open`/`mmap` plus a POSIX named semaphore used as a binary lock. Standard C++ mutexes and atomics are not assumed to be process-shared.
- Windows: `CreateFileMapping`/`MapViewOfFile` plus a named Win32 mutex. `WAIT_ABANDONED` poisons the session and returns `peer_failed`.
- Both backends serialize ring metadata and byte copies. The lock is held only for bounded in-memory work; the API's timeout covers lock contention and full/empty backpressure.
- No backend recovers a frame interrupted by process death. POSIX named semaphores have no owner-death recovery: if a process exits while holding the semaphore, calls against that name time out. On Windows an abandoned mutex is reported and the session is marked failed. In either case, discard the old name and create a fresh session. Do not unlink/recreate a name while peers still use it.
- Death while idle is not automatically detected. A surviving writer may fill the ring and time out; a surviving reader may time out waiting for a producer. Callers decide whether a timeout is ordinary pressure or a failed peer, then abandon/recreate when appropriate.

## Deliberate limits

V1 is SPSC per direction only: no MPMC claims, process-shared pointers, durable replay, live resize, cross-host shared memory, crash repair, or lock-free path. These are outside the correctness milestone.
