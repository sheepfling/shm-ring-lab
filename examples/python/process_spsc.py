#!/usr/bin/env python3
"""A four-slot process-shared ring: SharedMemory stores bytes, Condition syncs."""

from __future__ import annotations

import argparse
import struct
import traceback
from multiprocessing import Condition, Queue, get_context, shared_memory
from typing import Any
import unittest

SLOT_BYTES = 4  # one fixed-width unsigned message number, network byte order


class Ring:
    def __init__(self, ctx: Any, capacity: int = 4) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.memory = shared_memory.SharedMemory(create=True, size=capacity * SLOT_BYTES)
        try:
            self.read_index = ctx.RawValue("i", 0)
            self.write_index = ctx.RawValue("i", 0)
            self.count = ctx.RawValue("i", 0)
            self.closed = ctx.RawValue("b", 0)
            self.condition = ctx.Condition(ctx.Lock())
        except BaseException:
            self.memory.close()
            self.memory.unlink()
            raise

    @classmethod
    def attach(cls, name: str, capacity: int, read_index: Any, write_index: Any,
               count: Any, closed: Any, condition: Condition) -> "Ring":
        ring = cls.__new__(cls)
        ring.capacity = capacity
        ring.memory = shared_memory.SharedMemory(name=name)
        ring.read_index, ring.write_index = read_index, write_index
        ring.count, ring.closed, ring.condition = count, closed, condition
        return ring

    def put(self, number: int, timeout: float | None = None) -> bool:
        encoded = struct.pack("!I", number)  # prepare outside critical section
        with self.condition:
            available = self.condition.wait_for(
                lambda: self.count.value < self.capacity or self.closed.value,
                timeout=timeout,
            )
            if not available or self.closed.value:
                return False
            start = self.write_index.value * SLOT_BYTES
            self.memory.buf[start : start + SLOT_BYTES] = encoded
            self.write_index.value = (self.write_index.value + 1) % self.capacity
            self.count.value += 1
            self.condition.notify_all()
            return True

    def get(self, timeout: float | None = None) -> int | None:
        with self.condition:
            available = self.condition.wait_for(
                lambda: self.count.value > 0 or self.closed.value,
                timeout=timeout,
            )
            if not available or self.count.value == 0:  # empty or closed and drained
                return None
            start = self.read_index.value * SLOT_BYTES
            encoded = bytes(self.memory.buf[start : start + SLOT_BYTES])
            self.read_index.value = (self.read_index.value + 1) % self.capacity
            self.count.value -= 1
            self.condition.notify_all()
        # Decode/handle only after releasing the process-shared lock.
        return struct.unpack("!I", encoded)[0]

    def close_writer(self) -> None:
        with self.condition:
            self.closed.value = 1
            self.condition.notify_all()

    def close_mapping(self) -> None:
        self.memory.close()

    def unlink(self) -> None:
        self.memory.unlink()


def consume(name: str, capacity: int, read_index: Any, write_index: Any,
            count: Any, closed: Any, condition: Condition, output: Queue) -> None:
    ring = None
    try:
        ring = Ring.attach(name, capacity, read_index, write_index, count, closed, condition)
        values = []
        while (value := ring.get(timeout=5)) is not None:
            values.append(value)  # work on copied value outside the ring lock
        output.put(("ok", values))
    except BaseException:
        # Send a traceback to the parent before allowing the process to fail.
        output.put(("error", traceback.format_exc()))
        raise
    finally:
        if ring is not None:
            ring.close_mapping()


class RingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ring = Ring(get_context("spawn"))
        self.cleaned = False

    def tearDown(self) -> None:
        if self.cleaned:
            return
        try:
            self.ring.close_mapping()
        finally:
            self.ring.unlink()

    def test_empty_timeout(self) -> None:
        self.assertIsNone(self.ring.get(timeout=0.01))

    def test_full_timeout(self) -> None:
        for value in range(4):
            self.assertTrue(self.ring.put(value, timeout=0))
        self.assertFalse(self.ring.put(99, timeout=0.01))

    def test_fifo_and_wraparound(self) -> None:
        for value in range(4):
            self.assertTrue(self.ring.put(value, timeout=0))
        self.assertEqual([self.ring.get(timeout=0) for _ in range(4)], [0, 1, 2, 3])
        for value in range(4, 8):
            self.assertTrue(self.ring.put(value, timeout=0))
        self.assertEqual([self.ring.get(timeout=0) for _ in range(4)], [4, 5, 6, 7])

    def test_close_drains_then_rejects_writes(self) -> None:
        self.assertTrue(self.ring.put(7, timeout=0))
        self.ring.close_writer()
        self.assertEqual(self.ring.get(timeout=0), 7)
        self.assertIsNone(self.ring.get(timeout=0))
        self.assertFalse(self.ring.put(8, timeout=0))

    def test_cleanup_unlinks_shared_memory(self) -> None:
        name = self.ring.memory.name
        try:
            self.ring.close_mapping()
        finally:
            self.ring.unlink()
            self.cleaned = True
        with self.assertRaises(FileNotFoundError):
            shared_memory.SharedMemory(name=name)

    def test_spawned_round_trip_and_cleanup(self) -> None:
        output = get_context("spawn").Queue()
        child = get_context("spawn").Process(
            target=consume,
            args=(self.ring.memory.name, self.ring.capacity, self.ring.read_index,
                  self.ring.write_index, self.ring.count, self.ring.closed,
                  self.ring.condition, output),
        )
        started = False
        try:
            child.start()
            started = True
            for value in range(12):
                self.assertTrue(self.ring.put(value, timeout=5), f"put timed out at {value}")
            self.ring.close_writer()
            status, payload = output.get(timeout=7)
            child.join(timeout=3)
            self.assertFalse(child.is_alive())
            self.assertEqual(status, "ok", payload)
            self.assertEqual(child.exitcode, 0)
            self.assertEqual(payload, list(range(12)))
        finally:
            if started and child.is_alive():
                child.terminate()
                child.join(timeout=2)
            output.close()
            output.join_thread()


def demonstrate(ctx: Any) -> None:
    ring = Ring(ctx)
    output = ctx.Queue()
    child = ctx.Process(
        target=consume,
        args=(ring.memory.name, ring.capacity, ring.read_index, ring.write_index,
              ring.count, ring.closed, ring.condition, output),
    )
    started = False
    try:
        child.start()
        started = True
        for value in range(12):
            if not ring.put(value, timeout=5):
                raise TimeoutError(f"put timed out at {value}")
        ring.close_writer()
        status, received = output.get(timeout=7)
        child.join(timeout=3)
        if status != "ok":
            raise RuntimeError(f"spawned consumer failed:\n{received}")
        if child.is_alive() or child.exitcode != 0:
            raise RuntimeError(f"consumer exited unexpectedly: {child.exitcode}")
        if received != list(range(12)):
            raise AssertionError(f"unexpected process FIFO: {received}")
        print("parent put:", list(range(12)))
        print("child got:", received)
        print(f"PASS: process FIFO {received}")
    finally:
        if started and child.is_alive():
            child.terminate()
            child.join(timeout=2)
        elif started:
            child.join()
        output.close()
        output.join_thread()
        ring.close_mapping()
        ring.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="run deterministic assertions")
    args = parser.parse_args()
    context = get_context("spawn")
    if args.test:
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(RingTests)
        )
        raise SystemExit(not result.wasSuccessful())
    else:
        demonstrate(context)
