# Architecture

```mermaid
flowchart LR
    subgraph A[Process A]
        AAPI[Application API]
        AB[POSIX or Win32 backend]
    end
    subgraph Shared[One OS shared-memory session]
        M[Versioned fixed-width header]
        R[Bounded FIFO slots\nlength + copied bytes]
        L[Named OS lock]
    end
    subgraph B[Process B]
        BB[POSIX or Win32 backend]
        BAPI[Application API]
    end
    AAPI --> AB
    AB -->|map + acquire lock| Shared
    Shared -->|map + acquire lock| BB
    BB --> BAPI
    AAPI -->|copy one frame in| R
    R -->|copy one frame out| BAPI
    L -. protects header and slot/index updates .- M
    L -. protects header and slot/index updates .- R
```

For duplex communication, create a second session with its own ring and lock in the reverse direction. The public C++ API does not expose OS mapping handles. POSIX and Windows naming, mapping, locking, cleanup, and process-ID calls are isolated under `src/platform/`.
