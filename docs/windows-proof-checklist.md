# Windows proof checklist (PB-MSI)

This checklist is for a native Windows run. Do not treat macOS or Linux results as Windows validation.

## Prerequisites and build

- Windows 10/11 x64 with Visual Studio 2022 and the **Desktop development with C++** workload (MSVC and Windows SDK).
- CMake 3.21 or newer for preset schema v3.
- Run in an x64 Native Tools Command Prompt or a terminal where Visual Studio 2022 is installed.

From the repository root:

```bat
cmake --version
cl /Bv
cmake --preset windows-msvc-x64
cmake --build --preset windows-msvc-x64-release --parallel
ctest --preset windows-msvc-x64-release
```

The CTest executable runs eight bounded process scenarios: FIFO ordering/wraparound/shutdown, empty receive timeout, full-ring backpressure and idle peer exit, a process killed while holding the mutex, creator/opener initialization, duplicate role ownership, malformed protocol/frame checks, and two directional rings. On Windows, the dead-owner case must observe `WAIT_ABANDONED`, fail the old session, and use a fresh name; it does not repair a partially written frame.

## Separate-process message proof

In Command Prompt, prepare the expected three-line input:

```bat
if not exist showcase-data mkdir showcase-data
(echo oak& echo pine& echo maple) > showcase-data\input.txt
```

Open three Command Prompt windows in the repo root. Start the host first and leave its window open:

```bat
build\windows-msvc-x64\Release\shm_ring_demo.exe host pbproof 4 256
```

In the second window, run the consumer:

```bat
build\windows-msvc-x64\Release\shm_ring_demo.exe consume pbproof > showcase-data\received.txt
```

In the third, run the producer:

```bat
type showcase-data\input.txt | build\windows-msvc-x64\Release\shm_ring_demo.exe produce pbproof
```

After both peers exit, compare the bytes and check the count and SHA-256:

```bat
fc /b showcase-data\input.txt showcase-data\received.txt && powershell -NoProfile -Command "$inputPath='showcase-data\input.txt'; $outputPath='showcase-data\received.txt'; $count=@(Get-Content -LiteralPath $outputPath).Count; $inputHash=(Get-FileHash -Algorithm SHA256 -LiteralPath $inputPath).Hash; $outputHash=(Get-FileHash -Algorithm SHA256 -LiteralPath $outputPath).Hash; if($count -ne 3 -or $inputHash -ne $outputHash){ Write-Error 'count or SHA256 mismatch'; exit 1 }; Write-Output ('PASS messages=' + $count + ' SHA256=' + $inputHash)"
```

When the peers have exited and the checks pass, press Enter in the host window and remove the session:

```bat
build\windows-msvc-x64\Release\shm_ring_demo.exe cleanup pbproof
```

## Record the result

Record Windows edition/build, architecture, `cmake --version`, the `cl /Bv` compiler/SDK output, the CTest summary, and the SHA-256 line. Confirm the host remained alive until both peers exited. If any step fails, preserve its exact command and redacted output; do not report Windows as validated until this native run passes.

The protocol uses Win32 named mutexes and file mappings. No claim is made that standard C++ atomics or `std::mutex` are process-shared. Native validation is still required for the mapping/object behavior, process termination, and `WAIT_ABANDONED` handling.
