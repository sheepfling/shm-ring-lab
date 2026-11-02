#!/usr/bin/env python3
"""Run host, producer, and consumer as independent processes and verify data."""

from __future__ import annotations

import argparse
import hashlib
import selectors
import subprocess
import tempfile
import uuid
from pathlib import Path


def stop(process: subprocess.Popen[bytes] | None, label: str) -> None:
    if process is None or process.poll() is not None:
        return
    process.kill()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{label} did not stop after SIGKILL") from exc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("demo", type=Path)
    parser.add_argument("--messages", type=int, default=5000)
    args = parser.parse_args()
    if args.messages < 1:
        parser.error("--messages must be positive")
    if args.messages > 100000:
        parser.error("--messages must not exceed 100000")
    demo = args.demo.resolve()
    if not demo.is_file():
        parser.error(f"demo executable does not exist: {demo}")

    # A short random name avoids stale POSIX namespace collisions across runs.
    name = "proof" + uuid.uuid4().hex[:12]
    host: subprocess.Popen[bytes] | None = None
    consumer: subprocess.Popen[bytes] | None = None
    producer: subprocess.Popen[bytes] | None = None
    with tempfile.TemporaryDirectory(prefix="shm-ring-linux-proof-") as scratch:
        scratch_dir = Path(scratch)
        source = scratch_dir / "input.txt"
        received = scratch_dir / "received.txt"
        source_bytes = b"".join(f"frame-{index:06d}:independent-process\n".encode("ascii")
                                 for index in range(args.messages))
        source.write_bytes(source_bytes)

        try:
            host = subprocess.Popen([str(demo), "host", name, "8", "64"],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            if host.stdout is None:
                raise RuntimeError("host stdout pipe was not created")
            with selectors.DefaultSelector() as selector:
                selector.register(host.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=5):
                    raise RuntimeError("host did not publish its ready banner within 5 seconds")
                banner = host.stdout.readline().decode("utf-8", errors="replace").strip()
            if not banner.startswith(f"Hosting '{name}'"):
                detail = host.stderr.read().decode("utf-8", errors="replace").strip() if host.stderr else ""
                raise RuntimeError(f"host failed to publish the session: {banner} {detail}".strip())

            with received.open("wb") as output:
                consumer = subprocess.Popen([str(demo), "consume", name],
                                            stdout=output, stderr=subprocess.PIPE)
                with source.open("rb") as input_stream:
                    producer = subprocess.Popen([str(demo), "produce", name],
                                                stdin=input_stream, stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE)
                    try:
                        producer_out, producer_err = producer.communicate(timeout=30)
                    except subprocess.TimeoutExpired as exc:
                        raise RuntimeError("producer exceeded its 30-second deadline") from exc
                if producer.returncode != 0:
                    raise RuntimeError("producer failed: " + producer_err.decode("utf-8", errors="replace").strip())
                if producer_out:
                    raise RuntimeError("producer wrote unexpected standard output")

            try:
                _, consumer_err = consumer.communicate(timeout=30)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("consumer did not observe writer close within 30 seconds") from exc
            if consumer.returncode != 0:
                raise RuntimeError("consumer failed: " + consumer_err.decode("utf-8", errors="replace").strip())

            received_bytes = received.read_bytes()
            count = received_bytes.count(b"\n")
            input_hash = hashlib.sha256(source_bytes).hexdigest()
            received_hash = hashlib.sha256(received_bytes).hexdigest()
            if count != args.messages:
                raise RuntimeError(f"message count mismatch: expected {args.messages}, received {count}")
            if received_bytes != source_bytes or received_hash != input_hash:
                raise RuntimeError(f"payload integrity mismatch: input SHA256={input_hash}, output SHA256={received_hash}")

            print(f"PASS: separate producer/consumer processes; messages={count}; "
                  f"bytes={len(received_bytes)}; sha256={received_hash}")
        finally:
            stop(producer, "producer")
            stop(consumer, "consumer")
            if host is not None and host.poll() is None:
                try:
                    if host.stdin is not None:
                        host.stdin.write(b"\n")
                        host.stdin.flush()
                    host.wait(timeout=3)
                except (BrokenPipeError, subprocess.TimeoutExpired):
                    stop(host, "host")
            cleanup = subprocess.run([str(demo), "cleanup", name], check=False, timeout=3,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            if cleanup.returncode != 0:
                detail = cleanup.stderr.decode("utf-8", errors="replace").strip()
                raise RuntimeError(f"session cleanup failed: {detail}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(f"Linux process proof failed: {error}")
