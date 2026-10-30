# Python SPSC ring: locks first, native synchronization next

This tutorial is an educational companion to the C++ transport. It does not
replace that transport or claim to be lock-free. Both examples use four slots
and numbered, fixed-width messages so their traces are easy to follow.

## Baseline used

This local work is based on `feature/initial-work` at `db109ee`
(`docs: record Linux Docker proof results`), the fetched GitHub branch ref and
the last known published head supplied for this task. The clone's `main` was
the separate seed commit `951d5fa`. The unpublished `benchmark/paired-ipc`
commit `d71c352` was not present in this checkout and was not recreated.

## Fresh MacBook Air setup

The Python examples have no third-party dependencies and leave the system
Python alone. In Terminal:

```sh
python3 --version     # use Python 3.10 or newer
git --version
```

If Python is missing or too old, install a current signed macOS package from
[python.org](https://www.python.org/downloads/macos/); open a new Terminal and
check `python3 --version` again.

For a fresh checkout:

```sh
mkdir -p ~/GIT/sheepfling
git clone --branch feature/initial-work --single-branch https://github.com/sheepfling/shm-ring-lab.git ~/GIT/sheepfling/shm-ring-lab
cd ~/GIT/sheepfling/shm-ring-lab
```

If `~/GIT/sheepfling/shm-ring-lab` already exists, do not clone over it. Inspect it first:

```sh
git -C ~/GIT/sheepfling/shm-ring-lab status --short --branch
```

If it is not a Git checkout, or it has modifications or untracked files, leave it untouched and clone into another empty directory. If it is a clean checkout, fetch and select the tutorial branch explicitly:

```sh
git -C ~/GIT/sheepfling/shm-ring-lab fetch origin feature/initial-work
git -C ~/GIT/sheepfling/shm-ring-lab switch --detach FETCH_HEAD
```

Run the Python examples below; they need only Python. To build the existing
C++ project too, install Apple's Command Line Tools with
`xcode-select --install` if `clang++` is missing; Apple documents the
[command-line-tools install path](https://developer.apple.com/documentation/xcode/installing-the-command-line-tools).
Install CMake from the [official downloads](https://cmake.org/download/) if
`cmake --version` is unavailable. Then follow the existing macOS project
instructions:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

## Start here

Run the threaded example and its tests first. Read the `put` and `get` traces
as slot publication and release, then follow those same steps into the
process-shared version. The process example explicitly uses the portable
`spawn` start method.

All commands use the standard library (Python 3.10 or newer; checked here with
Python 3.12.3):

```sh
python3 examples/python/threaded_spsc.py
python3 examples/python/threaded_spsc.py --test
python3 examples/python/process_spsc.py
python3 examples/python/process_spsc.py --test
```

Normal runs print grouped traces after completion, so thread scheduling does
not scramble the teaching output. Each `--test` runs named `unittest` cases;
the process tests include a spawned child round-trip. The process worker is
defined at module scope and process creation is protected by the main guard,
as required by `spawn`.

## Stage 1: threads

[`../examples/python/threaded_spsc.py`](../examples/python/threaded_spsc.py)
uses a Python `Lock` under a `Condition`. The producer encodes a number before
locking, copies it into the current write slot, advances `write_index`, and
increments `count` while holding the lock. The consumer copies slot bytes out,
advances `read_index`, and decrements `count` under that lock. It decodes the
private copy after unlocking. `Condition.wait()` releases the lock while
blocked and reacquires it before checking the predicate again.

The tests cover empty and full timeouts, FIFO and wraparound, draining after
close, rejecting writes after close, and waking a blocked consumer on close.
The GIL does not establish the queue invariant: the explicit lock does.

## Stage 2: processes

[`../examples/python/process_spsc.py`](../examples/python/process_spsc.py)
uses `multiprocessing.shared_memory.SharedMemory` for four fixed-size slots
(16 bytes total). Each slot is one unsigned 32-bit number in network byte
order. `RawValue` instances store the two indices, count, and close flag. A
process-shared `Condition` and its lock guard both those indices and the slot
copies. The producer encodes before locking; the consumer decodes and handles
the private byte copy after unlocking.

SharedMemory is storage, not synchronization. The GIL is not shared between
processes. `RawValue` supplies shared storage, not atomic increments; the common
condition lock protects all access to its values and the slot bytes.

### Timeout scope

In both examples, `put` and `get` enter `with self.condition` before waiting.
Acquiring that lock has no per-call timeout. The threaded example sets its
deadline before acquiring but cannot interrupt a blocked acquire; the process
example starts `Condition.wait_for(timeout=...)` only after acquiring the
lock. `Condition.wait` and `wait_for` must reacquire the lock before returning.
These timeouts bound the queue wait after lock acquisition; contention during
acquisition or reacquisition can extend the call. A process killed while
holding the process-shared lock can leave its peer blocked indefinitely. This
differs from the native API's deadline-bounded lock acquisition. Neither
Python example recovers an abandoned lock; discard the process ring after
abnormal termination.

```mermaid
flowchart LR
    subgraph ring[Process-shared state]
        slots[SharedMemory: slot 0 | slot 1 | slot 2 | slot 3]
        state[RawValue: read_index, write_index, count, closed]
        lock[Condition + process-shared Lock]
    end
    producer[Producer process] -->|write slot then publish index/count| ring
    ring -->|copy slot then release index/count| consumer[Consumer process]
    lock -. guards .- slots
    lock -. guards .- state
```

For every wait, the condition checks a predicate while holding the same lock
used by writers. If the predicate is false, waiting atomically releases the
lock; after notification, the waiter reacquires it and checks again. This
avoids lost wakeups. Notifications alone do not publish data; the lock
protects the state changes and slot bytes.

The six process tests cover empty/full timeouts, FIFO/wraparound, close-and-
drain, unlink cleanup, and the spawned round-trip. The round-trip checks the
child's result and exit code. Error tracebacks are returned to the parent.

## Relating this to the native transport

The source-verified code in the published baseline is a locked queue, not a
lock-free variant. Its operations map as follows:

| Operation | Python example | Native C++ project |
| --- | --- | --- |
| Storage setup | Four 4-byte `SharedMemory` slots; `RawValue` indices/count/closed | Versioned `SharedHeader`, per-slot `SlotHeader`, payload bytes in [`protocol.hpp:18–36`](../src/protocol.hpp#L18) |
| Acquire before inspecting state | Enter the shared condition lock in `put` or `get` | `lock_until` then `platform::lock_for` in [`session.cpp:68–90`](../src/session.cpp#L68); backend contract [`backend.hpp:30–31`](../src/platform/backend.hpp#L30) |
| Producer write and publish | Encode before lock; copy slot bytes, advance write index, increment count, notify | Copy length and payload, advance write index, increment count under the platform lock in [`session.cpp:366–376`](../src/session.cpp#L366) |
| Consumer read and release | Copy slot to private bytes, advance read index, decrement count, notify; decode outside lock | Copy into private `Message`, advance read index, decrement count in [`session.cpp:404–418`](../src/session.cpp#L404) |
| Full/empty wait | `Condition.wait` / `Condition.wait_for` releases and reacquires; timeout does not bound condition-lock acquisition/reacquisition | Unlock, poll with the operation deadline, reacquire using that deadline in [`session.cpp:379–382`](../src/session.cpp#L379) and [`session.cpp:424–427`](../src/session.cpp#L424) |
| Normal end of stream | Set closed under lock and notify; child drains, exits, parent joins and unlinks | `close_writer()` sets `writer_closed`; receive drains then reports closed in [`session.cpp:430–448`](../src/session.cpp#L430) |

The native acquire side is successful `lock_for`: POSIX calls `sem_trywait`
and Windows calls `WaitForSingleObject`. The release side is `unlock`: POSIX
calls `sem_post` and Windows calls `ReleaseMutex` ([`backend_posix.cpp:154–166`](../src/platform/backend_posix.cpp#L154), [`backend_windows.cpp:151–160`](../src/platform/backend_windows.cpp#L151)). These are the two sides of this OS lock protocol. They are not C++ atomic acquire/release operations.

This Python ring is **not binary-compatible** with C++. Python stores four
four-byte numbers in its mapping and keeps indices and lock objects in separate
`multiprocessing` objects. C++ uses a versioned header, per-slot lengths and
variable-size payloads, and separate named OS synchronization. They share the
bounded-FIFO teaching concept only.

## Shutdown and termination limits

For a normal shutdown, the parent finishes writes, sets `closed` under the
condition lock, and notifies waiters. The child drains queued values, returns
its result, and exits. The parent checks the result and exit code, joins the
child, then closes and unlinks its mapping; the child closes its own
attachment. The tests verify child exit and explicit unlink cleanup.

`Process.terminate()` is only failure cleanup if the child is still alive; it
does not provide recovery. If a process is killed while holding the shared
lock, another process may wait indefinitely. The ring does not repair indices,
reclaim locks, or recover a partially updated slot. Even when killed outside
the critical section, the process can skip its `finally` block. Python's
resource tracker helps with resource cleanup, but does not restore queue
consistency. Discard a ring after abnormal termination.

The native baseline has the same no-recovery boundary: its README notes that
the POSIX named semaphore cannot recover a permit left unavailable by a dead
process; the Windows backend detects an abandoned mutex and marks the session
failed. Neither backend resumes a damaged session.

## Lock-free options in Python

There is no portable pure-Python interprocess atomic API used here. The GIL
coordinates Python execution within an interpreter, not separate processes;
and it is not a synchronization contract for future thread modes. CPython's
free-threaded direction is described in [PEP 703](https://peps.python.org/pep-0703/).
Python's [SharedMemory API](https://docs.python.org/3/library/multiprocessing.shared_memory.html)
allocates/attaches shared bytes and documents `close()` / `unlink()` lifecycle
operations. It does not provide atomic access. Python's
[sharedctypes docs](https://docs.python.org/3/library/multiprocessing.html#the-multiprocessing-sharedctypes-module)
warn that raw array element access can be non-atomic; this example therefore
guards all `RawValue` and slot access with one process-shared lock.

Practical choices:

1. Use the Python process-shared lock and condition shown here. This is the
   straightforward choice and is not lock-free.
2. Put a carefully defined ring protocol in a C/C++ extension or shared
   library, called from Python using [ctypes](https://docs.python.org/3/library/ctypes.html)
   or a binding. `ctypes` provides the call boundary only; native code must
   supply synchronization and ABI guarantees.
3. For lock-free operations, the native implementation needs aligned atomic
   indices, correct publication/reclamation memory ordering, per-type/object
   lock-free checks, and an address-free guarantee for shared mappings. The
   [C++ draft lock-free property](https://eel.is/c++draft/atomics.lockfree)
   says operations may only sometimes be lock-free and makes address-free
   behavior recommended practice; that is not a blanket cross-platform
   interprocess promise. See also the [C++ draft ordering rules](https://eel.is/c++draft/atomics.order).
   Validate the exact compiler, ABI, mapping layout, alignment, termination,
   and target platforms before claiming a lock-free process ring.

This tutorial implements only the first choice. It adds no native bindings or
lock-free backend.

## Validation status for this checkout

Validated on macOS with Python 3.12.3, Apple Clang 21.0.0, and CMake 4.4.4.
The threaded suite passed 5 named cases; the process suite passed all 6 named
cases, including the spawned child's clean exit and shared-memory unlink
cleanup. Both demos passed FIFO ordering. The native C++ CTest entry passed all
8 correctness scenarios.

The initial default-sandbox process and CTest attempts returned
`PermissionError` / `EPERM` from shared-memory creation. The exact commands
then passed after narrowly scoped command approvals ran them outside that
execution sandbox, on the same Mac. This isolates the earlier denial to the
restricted execution context; no OS security settings were changed. The
process commands validated were:

```sh
python3 examples/python/process_spsc.py
python3 examples/python/process_spsc.py --test
```

The native C++ build had already succeeded. CTest's one registered test
contains eight scenarios; all eight passed with the approved invocation:

```sh
ctest --test-dir /tmp/shm-ring-lab-build --output-on-failure
```

The build directory was `/tmp/shm-ring-lab-build`; Python was
`/Library/Frameworks/Python.framework/Versions/3.12/bin/python3`. The temporary
build output is outside the repository.
