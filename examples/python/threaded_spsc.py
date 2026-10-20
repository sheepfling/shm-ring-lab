#!/usr/bin/env python3
"""A four-slot, single-producer/single-consumer ring using a Condition."""

from __future__ import annotations

import argparse
import struct
import sys
import threading
import time
import unittest


class Ring:
    def __init__(self, capacity: int = 4) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.slots = [bytearray(4) for _ in range(capacity)]
        self.read_index = self.write_index = self.count = 0
        self.closed = False
        self.condition = threading.Condition(threading.Lock())

    def put(self, number: int, timeout: float | None = None) -> bool:
        # Prepare the message before taking the shared-state lock.
        encoded = struct.pack("!I", number)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while self.count == self.capacity and not self.closed:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self.condition.wait(remaining)
            if self.closed:
                return False
            self.slots[self.write_index][:] = encoded
            self.write_index = (self.write_index + 1) % self.capacity
            self.count += 1
            self.condition.notify_all()
            return True

    def get(self, timeout: float | None = None) -> int | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while self.count == 0 and not self.closed:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                self.condition.wait(remaining)
            if self.count == 0:  # closed and drained
                return None
            encoded = bytes(self.slots[self.read_index])
            self.read_index = (self.read_index + 1) % self.capacity
            self.count -= 1
            self.condition.notify_all()
        # Decode/handle the message after releasing the queue lock.
        return struct.unpack("!I", encoded)[0]

    def close(self) -> None:
        with self.condition:
            self.closed = True
            self.condition.notify_all()


class RingTests(unittest.TestCase):
    def test_empty_timeout(self) -> None:
        self.assertIsNone(Ring().get(timeout=0.01))

    def test_full_timeout(self) -> None:
        ring = Ring()
        for value in range(4):
            self.assertTrue(ring.put(value, timeout=0))
        self.assertFalse(ring.put(99, timeout=0.01))

    def test_fifo_and_wraparound(self) -> None:
        ring = Ring()
        for value in range(4):
            self.assertTrue(ring.put(value, timeout=0))
        self.assertEqual([ring.get(timeout=0) for _ in range(4)], [0, 1, 2, 3])
        for value in range(4, 8):
            self.assertTrue(ring.put(value, timeout=0))
        self.assertEqual([ring.get(timeout=0) for _ in range(4)], [4, 5, 6, 7])

    def test_close_drains_then_rejects_writes(self) -> None:
        ring = Ring()
        self.assertTrue(ring.put(7, timeout=0))
        ring.close()
        self.assertEqual(ring.get(timeout=0), 7)
        self.assertIsNone(ring.get(timeout=0))
        self.assertFalse(ring.put(8, timeout=0))

    def test_close_wakes_waiting_consumer(self) -> None:
        ring = Ring()
        entering_wait = threading.Event()
        original_wait = ring.condition.wait

        def observed_wait(timeout: float | None = None) -> bool:
            entering_wait.set()  # set while holding the condition lock
            return original_wait(timeout)

        ring.condition.wait = observed_wait
        received: list[int | None] = []
        reader = threading.Thread(target=lambda: received.append(ring.get()))
        reader.start()
        self.assertTrue(entering_wait.wait(timeout=2))
        ring.close()  # acquires only after wait atomically releases/registers
        reader.join(timeout=2)
        self.assertFalse(reader.is_alive())
        self.assertEqual(received, [None])


def demonstrate() -> None:
    ring = Ring()
    received: list[int] = []

    def producer() -> None:
        for value in range(8):
            assert ring.put(value, timeout=2)
        ring.close()

    def consumer() -> None:
        while (value := ring.get(timeout=2)) is not None:
            received.append(value)

    writer = threading.Thread(target=producer)
    reader = threading.Thread(target=consumer)
    writer.start()
    reader.start()
    writer.join(timeout=3)
    reader.join(timeout=3)
    assert not writer.is_alive() and not reader.is_alive()
    assert received == list(range(8))
    print("producer put:", list(range(8)))
    print("consumer got:", received)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="run deterministic assertions")
    args = parser.parse_args()
    if args.test:
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(RingTests)
        )
        raise SystemExit(not result.wasSuccessful())
    else:
        demonstrate()
