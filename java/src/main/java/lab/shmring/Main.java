package lab.shmring;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public final class Main {
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(3);

    private Main() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "selftest".equals(args[0])) {
            selftest();
        } else if ("host".equals(args[0]) && args.length == 4) {
            host(Path.of(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        } else if ("produce".equals(args[0]) && args.length == 3) {
            produce(Path.of(args[1]), Integer.parseInt(args[2]));
        } else if ("consume".equals(args[0]) && args.length == 3) {
            consume(Path.of(args[1]), Integer.parseInt(args[2]));
        } else if ("hold-lock".equals(args[0]) && args.length == 4) {
            holdLock(Path.of(args[1]), Path.of(args[2]), Long.parseLong(args[3]));
        } else {
            System.err.println("usage: Main selftest | host <file> <capacity> <max-bytes> | produce <file> <count> | consume <file> <count>");
            System.exit(2);
        }
    }

    private static void host(Path path, int capacity, int maxBytes) throws Exception {
        try (FileRing ring = FileRing.create(path, capacity, maxBytes)) {
            System.out.println("READY " + path.toAbsolutePath());
            System.out.flush();
            new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)).readLine();
            require(ring.closeWriter(OP_TIMEOUT) == FileRing.Status.OK, "could not close writer");
        }
    }

    private static void produce(Path path, int count) throws Exception {
        try (FileRing ring = FileRing.open(path)) {
            for (int i = 0; i < count; i++) {
                byte[] message = ("item-" + i).getBytes(StandardCharsets.UTF_8);
                require(ring.send(message, OP_TIMEOUT) == FileRing.Status.OK, "send timed out at item " + i);
            }
            require(ring.closeWriter(OP_TIMEOUT) == FileRing.Status.OK, "close writer timed out");
            System.out.println("PASS sent=" + count);
        }
    }

    private static void consume(Path path, int count) throws Exception {
        try (FileRing ring = FileRing.open(path)) {
            for (int i = 0; i < count; i++) {
                FileRing.Receive result = ring.receive(OP_TIMEOUT);
                require(result.status == FileRing.Status.OK, "receive failed at item " + i + ": " + result.status);
                String actual = new String(result.payload, StandardCharsets.UTF_8);
                require(actual.equals("item-" + i), "out-of-order message at item " + i + ": " + actual);
            }
            require(ring.receive(OP_TIMEOUT).status == FileRing.Status.END_OF_STREAM,
                    "consumer did not observe clean end-of-stream");
            System.out.println("PASS consumed=" + count);
        }
    }

    private static void selftest() throws Exception {
        Path directory = Files.createTempDirectory("java-ring-lab-");
        Path localPath = directory.resolve("local.ring");
        try (FileRing ring = FileRing.create(localPath, 2, 16)) {
            require(ring.send(bytes("immediate"), Duration.ZERO) == FileRing.Status.OK,
                    "zero-deadline send did not try an available slot");
            require(text(ring.receive(Duration.ZERO)).equals("immediate"),
                    "zero-deadline receive did not read an available message");
            require(ring.send(null, OP_TIMEOUT) == FileRing.Status.INVALID_ARGUMENT,
                    "null payload was not rejected");
            require(ring.send(bytes("a"), OP_TIMEOUT) == FileRing.Status.OK, "first enqueue failed");
            require(ring.send(bytes("b"), OP_TIMEOUT) == FileRing.Status.OK, "second enqueue failed");
            require(ring.send(bytes("c"), Duration.ofMillis(30)) == FileRing.Status.TIMEOUT,
                    "full-ring send did not time out");
            require(text(ring.receive(OP_TIMEOUT)).equals("a"), "FIFO read 1 failed");
            require(ring.send(bytes("c"), OP_TIMEOUT) == FileRing.Status.OK, "wraparound enqueue failed");
            require(text(ring.receive(OP_TIMEOUT)).equals("b"), "FIFO read 2 failed");
            require(text(ring.receive(OP_TIMEOUT)).equals("c"), "FIFO wraparound read failed");
            require(ring.closeWriter(OP_TIMEOUT) == FileRing.Status.OK, "close writer failed");
            require(ring.send(bytes("closed"), OP_TIMEOUT) == FileRing.Status.CLOSED,
                    "send after writer close was accepted");
            require(ring.receive(OP_TIMEOUT).status == FileRing.Status.END_OF_STREAM, "clean close failed");
        }
        Files.delete(localPath);

        Path processPath = directory.resolve("process.ring");
        try (FileRing owner = FileRing.create(processPath, 16, 32)) {
            require(owner.capacity() == 16, "owner did not retain the expected ring layout");
            require(owner.send(new byte[33], OP_TIMEOUT) == FileRing.Status.TOO_LARGE,
                    "oversized message was not rejected");
            long emptyStart = System.nanoTime();
            require(owner.receive(Duration.ofMillis(30)).status == FileRing.Status.TIMEOUT,
                    "empty-ring receive did not time out");
            require(Duration.ofNanos(System.nanoTime() - emptyStart).toMillis() < 750,
                    "empty-ring timeout exceeded its 750 ms test bound");
            Path lockMarker = directory.resolve("locked.ready");
            Process lockHolder = child("hold-lock", processPath.toString(), lockMarker.toString(), "500");
            waitForFile(lockMarker, Duration.ofSeconds(3));
            long lockStart = System.nanoTime();
            require(owner.send(bytes("contended"), Duration.ofMillis(50)) == FileRing.Status.TIMEOUT,
                    "file-lock acquisition did not time out");
            require(Duration.ofNanos(System.nanoTime() - lockStart).toMillis() < 750,
                    "file-lock timeout exceeded its 750 ms test bound");
            await("lock-holder", lockHolder);
            Files.delete(lockMarker);

            Process consumer = child("consume", processPath.toString(), "500");
            Process producer = child("produce", processPath.toString(), "500");
            await("producer", producer);
            await("consumer", consumer);
        }
        Files.delete(processPath);
        Files.delete(directory);
        System.out.println("PASS FIFO/wraparound, full/empty deadlines, lock contention, close, and two-process transfer");
    }

    private static void holdLock(Path path, Path marker, long millis) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileLock lock = channel.lock(0, 1, false)) {
            require(lock.isValid(), "test lock was not acquired");
            Files.writeString(marker, "locked\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            Thread.sleep(millis);
        }
        System.out.println("PASS lock released");
    }

    private static void waitForFile(Path path, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!Files.exists(path)) {
            if (System.nanoTime() >= deadline) throw new IllegalStateException("file-lock handshake timed out");
            Thread.sleep(5);
        }
    }

    private static Process child(String... args) throws Exception {
        String javaName = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        Path java = Path.of(System.getProperty("java.home"), "bin", javaName);
        String[] command = new String[5 + args.length];
        command[0] = java.toString();
        command[1] = "-XX:-UsePerfData";
        command[2] = "-cp";
        command[3] = System.getProperty("java.class.path");
        command[4] = Main.class.getName();
        System.arraycopy(args, 0, command, 5, args.length);
        return new ProcessBuilder(command).redirectErrorStream(true).start();
    }

    private static void await(String name, Process process) throws Exception {
        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(3, TimeUnit.SECONDS);
            throw new IllegalStateException(name + " exceeded the 15-second process deadline");
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        require(process.exitValue() == 0, name + " failed: " + output);
        require(output.startsWith("PASS "), name + " returned unexpected output: " + output);
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String text(FileRing.Receive result) {
        require(result.status == FileRing.Status.OK, "receive failed: " + result.status);
        return new String(result.payload, StandardCharsets.UTF_8);
    }
    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
