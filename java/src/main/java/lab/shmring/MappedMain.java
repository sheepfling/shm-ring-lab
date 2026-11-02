package lab.shmring;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Process harness for the actual shared mapped-file backend. */
public final class MappedMain {
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(3);

    private MappedMain() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "selftest".equals(args[0])) {
            selftest();
        } else if ("scenario".equals(args[0]) && args.length == 2) {
            scenario(Path.of(args[1]));
        } else if ("large-map".equals(args[0]) && args.length == 2) {
            largeMapTest(Path.of(args[1]));
        } else if ("host-test".equals(args[0]) && args.length == 6) {
            hostTest(Path.of(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]),
                    Path.of(args[4]), Path.of(args[5]));
        } else if ("produce".equals(args[0]) && args.length == 3) {
            produce(Path.of(args[1]), Integer.parseInt(args[2]));
        } else if ("consume".equals(args[0]) && args.length == 3) {
            consume(Path.of(args[1]), Integer.parseInt(args[2]));
        } else if ("hold-lock".equals(args[0]) && args.length == 4) {
            holdLock(Path.of(args[1]), Path.of(args[2]), Long.parseLong(args[3]));
        } else if ("probe-lock".equals(args[0]) && args.length == 2) {
            probeLock(Path.of(args[1]));
        } else {
            System.err.println("usage: MappedMain selftest | scenario <file> | host-test <file> <capacity> <max-bytes> <ready> <stop> | produce <file> <count> | consume <file> <count> | hold-lock <file> <ready> <ms> | probe-lock <file>");
            System.exit(2);
        }
    }

    private static void scenario(Path path) throws Exception {
        try (MappedFileRing ring = MappedFileRing.create(path, 2, 16)) {
            require(ring.capacity() == 2, "wrong mapped capacity");
            require(ring.send(bytes("immediate"), Duration.ZERO) == MappedFileRing.Status.OK,
                    "zero-deadline send missed an available slot");
            require(text(ring.receive(Duration.ZERO)).equals("immediate"),
                    "zero-deadline receive missed an available message");
            require(ring.send(null, OP_TIMEOUT) == MappedFileRing.Status.INVALID_ARGUMENT,
                    "null payload was accepted");
            require(ring.send(new byte[17], OP_TIMEOUT) == MappedFileRing.Status.TOO_LARGE,
                    "oversized payload was accepted");

            long emptyStart = System.nanoTime();
            require(ring.receive(Duration.ofMillis(30)).status == MappedFileRing.Status.TIMEOUT,
                    "empty receive did not time out");
            require(elapsedMillis(emptyStart) < 750, "empty timeout exceeded 750 ms");

            require(ring.send(bytes("a"), OP_TIMEOUT) == MappedFileRing.Status.OK, "enqueue a failed");
            require(ring.send(bytes("b"), OP_TIMEOUT) == MappedFileRing.Status.OK, "enqueue b failed");
            long fullStart = System.nanoTime();
            require(ring.send(bytes("c"), Duration.ofMillis(30)) == MappedFileRing.Status.TIMEOUT,
                    "full ring did not time out");
            require(elapsedMillis(fullStart) < 750, "full timeout exceeded 750 ms");
            require(text(ring.receive(OP_TIMEOUT)).equals("a"), "FIFO receive a failed");
            require(ring.send(bytes("c"), OP_TIMEOUT) == MappedFileRing.Status.OK, "wraparound enqueue failed");
            require(text(ring.receive(OP_TIMEOUT)).equals("b"), "FIFO receive b failed");
            require(text(ring.receive(OP_TIMEOUT)).equals("c"), "wraparound receive failed");
            require(ring.closeWriter(OP_TIMEOUT) == MappedFileRing.Status.OK, "writer close failed");
            require(ring.send(bytes("after-close"), OP_TIMEOUT) == MappedFileRing.Status.CLOSED,
                    "send after close was accepted");
            require(ring.receive(OP_TIMEOUT).status == MappedFileRing.Status.END_OF_STREAM,
                    "end-of-stream was not observed");
        }
        System.out.println("PASS mapped-file FIFO, wraparound, deadlines, validation, and close");
        concurrentCloseTest(path.resolveSibling("close-race.mapped"));
    }

    private static void concurrentCloseTest(Path path) throws Exception {
        MappedFileRing ring = MappedFileRing.create(path, 2, 16);
        require(ring.send(bytes("one"), OP_TIMEOUT) == MappedFileRing.Status.OK, "close-race enqueue one failed");
        require(ring.send(bytes("two"), OP_TIMEOUT) == MappedFileRing.Status.OK, "close-race enqueue two failed");
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<MappedFileRing.Status> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread sender = new Thread(() -> {
            started.countDown();
            try {
                result.set(ring.send(bytes("blocked"), Duration.ofMillis(600)));
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "mapped-close-race-sender");
        sender.start();
        try {
            require(started.await(2, TimeUnit.SECONDS), "close-race sender did not start");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (sender.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            require(sender.getState() == Thread.State.TIMED_WAITING,
                    "close-race sender did not enter its bounded full-ring wait");
            long closeStart = System.nanoTime();
            ring.close();
            long closeElapsed = elapsedMillis(closeStart);
            sender.join(1500);
            require(!sender.isAlive(), "close-race sender did not finish after close waited");
            require(failure.get() == null, "close interrupted an active operation: " + failure.get());
            require(result.get() == MappedFileRing.Status.TIMEOUT, "active send did not finish with timeout");
            require(closeElapsed >= 100 && closeElapsed < 2000,
                    "close did not wait for the active send deadline: " + closeElapsed + " ms");
            boolean closedOperationRejected = false;
            try {
                ring.send(bytes("after-close"), Duration.ZERO);
            } catch (IOException expected) {
                closedOperationRejected = expected.getMessage().contains("mapped session is closed");
            }
            require(closedOperationRejected, "operation after close was not rejected as closed");
            System.out.println("PASS close waits for active operation and rejects later sends (" + closeElapsed + " ms)");
        } finally {
            if (sender.isAlive()) {
                ring.close();
                sender.join(1500);
                if (sender.isAlive()) {
                    sender.interrupt();
                    sender.join(1500);
                }
            }
            ring.close();
        }
    }

    private static void largeMapTest(Path path) throws Exception {
        int maxPayload = 16 * 1024 * 1024;
        try (MappedFileRing ring = MappedFileRing.create(path, 2, maxPayload)) {
            require(ring.maxPayloadBytes() == maxPayload, "large mapped capacity mismatch");
        }
        try (MappedFileRing ring = MappedFileRing.open(path)) {
            long start = System.nanoTime();
            require(ring.receive(Duration.ofMillis(20)).status == MappedFileRing.Status.TIMEOUT,
                    "large mapped empty receive did not time out");
            long elapsed = elapsedMillis(start);
            require(elapsed < 2000, "large mapped poll exceeded 2 seconds");
            require(ring.send(bytes("sparse-map"), OP_TIMEOUT) == MappedFileRing.Status.OK,
                    "large mapped send failed");
            require(text(ring.receive(OP_TIMEOUT)).equals("sparse-map"), "large mapped receive failed");
            System.out.println("PASS 32 MiB mapped-file poll and transfer (" + elapsed + " ms timeout call)");
        }
    }

    private static void hostTest(Path path, int capacity, int maxBytes, Path ready, Path stop) throws Exception {
        try (MappedFileRing ring = MappedFileRing.create(path, capacity, maxBytes)) {
            Files.writeString(ready, "ready\n", StandardCharsets.UTF_8);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (!Files.exists(stop)) {
                if (System.nanoTime() >= deadline) throw new IllegalStateException("host stop deadline expired");
                Thread.sleep(5);
            }
            require(ring.closeWriter(OP_TIMEOUT) == MappedFileRing.Status.OK, "host close-writer timed out");
        }
        System.out.println("PASS mapped host closed");
    }

    private static void produce(Path path, int count) throws Exception {
        try (MappedFileRing ring = MappedFileRing.open(path)) {
            for (int i = 0; i < count; i++) {
                byte[] payload = bytes("mapped-" + i);
                require(ring.send(payload, OP_TIMEOUT) == MappedFileRing.Status.OK,
                        "mapped send timed out at item " + i);
            }
            require(ring.closeWriter(OP_TIMEOUT) == MappedFileRing.Status.OK, "mapped writer close timed out");
        }
        System.out.println("PASS mapped sent=" + count);
    }

    private static void consume(Path path, int count) throws Exception {
        try (MappedFileRing ring = MappedFileRing.open(path)) {
            for (int i = 0; i < count; i++) {
                MappedFileRing.Receive result = ring.receive(OP_TIMEOUT);
                require(result.status == MappedFileRing.Status.OK, "mapped receive failed at " + i + ": " + result.status);
                require(text(result).equals("mapped-" + i), "mapped FIFO order failed at item " + i);
            }
            require(ring.receive(OP_TIMEOUT).status == MappedFileRing.Status.END_OF_STREAM,
                    "mapped end-of-stream was not observed");
        }
        System.out.println("PASS mapped consumed=" + count);
    }

    private static void holdLock(Path path, Path ready, long millis) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileLock lock = channel.lock(0, 1, false)) {
            require(lock.isValid(), "mapped test lock was not acquired");
            Files.writeString(ready, "locked\n", StandardCharsets.UTF_8);
            Thread.sleep(millis);
        }
        System.out.println("PASS mapped lock released");
    }

    private static void probeLock(Path path) throws Exception {
        try (MappedFileRing ring = MappedFileRing.open(path)) {
            long start = System.nanoTime();
            require(ring.send(bytes("contended"), Duration.ofMillis(50)) == MappedFileRing.Status.TIMEOUT,
                    "mapped operation did not respect file-lock contention");
            require(elapsedMillis(start) < 750, "mapped lock timeout exceeded 750 ms");
        }
        System.out.println("PASS mapped lock-contention deadline");
    }

    private static void selftest() throws Exception {
        Path directory = Files.createTempDirectory("java-mapped-ring-");
        List<Process> children = new ArrayList<>();
        Path local = directory.resolve("local.mapped");
        Path session = directory.resolve("process.mapped");
        Path ready = directory.resolve("host.ready");
        Path stop = directory.resolve("host.stop");
        try {
            Process localTest = child("scenario", local.toString());
            children.add(localTest);
            String scenarioOutput = await("mapped scenario", localTest, Duration.ofSeconds(10));
            require(scenarioOutput.contains("PASS close waits for active operation"),
                    "mapped close-race regression did not report its pass");
            Files.delete(local); // subprocess exit proves its mapping lifetime has ended
            Files.delete(directory.resolve("close-race.mapped"));

            Path large = directory.resolve("large.mapped");
            Process largeTest = child("large-map", large.toString());
            children.add(largeTest);
            String largeOutput = await("large mapped poll", largeTest, Duration.ofSeconds(10));
            require(largeOutput.contains("PASS 32 MiB mapped-file poll"),
                    "large mapping check did not report its pass");
            Files.delete(large);

            Process host = child("host-test", session.toString(), "16", "32", ready.toString(), stop.toString());
            children.add(host);
            waitForFile(ready, Duration.ofSeconds(5));

            Path lockReady = directory.resolve("lock.ready");
            Process lockHolder = child("hold-lock", session.toString(), lockReady.toString(), "500");
            children.add(lockHolder);
            waitForFile(lockReady, Duration.ofSeconds(5));
            Process lockProbe = child("probe-lock", session.toString());
            children.add(lockProbe);
            await("mapped lock probe", lockProbe, Duration.ofSeconds(5));
            await("mapped lock holder", lockHolder, Duration.ofSeconds(5));
            Files.delete(lockReady);

            Process consumer = child("consume", session.toString(), "500");
            children.add(consumer);
            Process producer = child("produce", session.toString(), "500");
            children.add(producer);
            await("mapped producer", producer, Duration.ofSeconds(15));
            await("mapped consumer", consumer, Duration.ofSeconds(15));
            Files.writeString(stop, "stop\n", StandardCharsets.UTF_8);
            await("mapped host", host, Duration.ofSeconds(5));
            Files.delete(session); // all three JVMs have exited, including the creator
            Files.delete(ready);
            Files.delete(stop);
            Files.delete(directory);
            System.out.println("PASS mapped transfer across three JVMs, 32 MiB poll, close-race, and post-exit cleanup");
        } finally {
            for (Process child : children) terminate(child);
            if (Files.exists(stop)) Files.deleteIfExists(stop);
            Files.deleteIfExists(ready);
            Files.deleteIfExists(session);
            Files.deleteIfExists(local);
            try { Files.deleteIfExists(directory); } catch (IOException ignored) { }
        }
    }

    private static Process child(String... args) throws IOException {
        String javaName = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        Path java = Path.of(System.getProperty("java.home"), "bin", javaName);
        String[] command = new String[5 + args.length];
        command[0] = java.toString();
        command[1] = "-XX:-UsePerfData";
        command[2] = "-cp";
        command[3] = System.getProperty("java.class.path");
        command[4] = MappedMain.class.getName();
        System.arraycopy(args, 0, command, 5, args.length);
        return new ProcessBuilder(command).redirectErrorStream(true).start();
    }

    private static String await(String name, Process process, Duration timeout) throws Exception {
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            process.waitFor(3, TimeUnit.SECONDS);
            throw new IllegalStateException(name + " exceeded " + timeout.toSeconds() + " seconds");
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        require(process.exitValue() == 0, name + " failed: " + output);
        require(output.startsWith("PASS "), name + " returned unexpected output: " + output);
        return output;
    }

    private static void terminate(Process process) throws InterruptedException {
        if (process.isAlive()) {
            process.destroyForcibly();
            process.waitFor(3, TimeUnit.SECONDS);
        }
    }

    private static void waitForFile(Path path, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!Files.exists(path)) {
            if (System.nanoTime() >= deadline) throw new IllegalStateException("mapped host readiness timed out");
            Thread.sleep(5);
        }
    }

    private static long elapsedMillis(long start) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start); }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String text(MappedFileRing.Receive result) {
        require(result.status == MappedFileRing.Status.OK, "mapped receive failed: " + result.status);
        return new String(result.payload, StandardCharsets.UTF_8);
    }
    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
