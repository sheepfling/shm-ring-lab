package lab.shmring;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/** A bounded, copied-message SPSC ring backed by a shared file. */
public final class FileRing implements AutoCloseable {
    public enum Status { OK, TIMEOUT, TOO_LARGE, CLOSED, END_OF_STREAM, INVALID_ARGUMENT, INVALID_FRAME }

    public static final class Receive {
        public final Status status;
        public final byte[] payload;

        private Receive(Status status, byte[] payload) {
            this.status = status;
            this.payload = payload;
        }
    }

    private static final int MAGIC = 0x4a524c31; // JRL1
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 64;
    private static final int MAGIC_OFFSET = 0;
    private static final int VERSION_OFFSET = 4;
    private static final int CAPACITY_OFFSET = 8;
    private static final int MAX_PAYLOAD_OFFSET = 12;
    private static final int WRITE_SEQUENCE_OFFSET = 16;
    private static final int READ_SEQUENCE_OFFSET = 24;
    private static final int WRITER_CLOSED_OFFSET = 32;
    private static final ConcurrentHashMap<Path, ReentrantLock> LOCAL_LOCKS = new ConcurrentHashMap<>();

    private final Path path;
    private final FileChannel channel;
    private final ReentrantLock localLock;
    private final int capacity;
    private final int maxPayload;
    private final long fileBytes;

    private FileRing(Path path, FileChannel channel, int capacity, int maxPayload) {
        this.path = path.toAbsolutePath().normalize();
        this.channel = channel;
        this.capacity = capacity;
        this.maxPayload = maxPayload;
        this.fileBytes = HEADER_BYTES + (long) capacity * (Integer.BYTES + (long) maxPayload);
        this.localLock = LOCAL_LOCKS.computeIfAbsent(this.path, ignored -> new ReentrantLock());
    }

    /** Create a new session file. Remove it only after every peer has exited and closed it. */
    public static FileRing create(Path path, int capacity, int maxPayload) throws IOException {
        if (path == null || capacity < 2 || maxPayload < 1) {
            throw new IllegalArgumentException("path, capacity >= 2, and maxPayload >= 1 are required");
        }
        long bytes = HEADER_BYTES + (long) capacity * (Integer.BYTES + (long) maxPayload);
        if (bytes > Integer.MAX_VALUE) throw new IllegalArgumentException("session file exceeds 2 GiB");
        Path normalized = path.toAbsolutePath().normalize();
        FileChannel channel = FileChannel.open(normalized, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            channel.position(bytes - 1);
            writeFully(channel, ByteBuffer.wrap(new byte[] {0}), bytes - 1);
            writeInt(channel, MAGIC_OFFSET, MAGIC);
            writeInt(channel, VERSION_OFFSET, VERSION);
            writeInt(channel, CAPACITY_OFFSET, capacity);
            writeInt(channel, MAX_PAYLOAD_OFFSET, maxPayload);
            writeLong(channel, WRITE_SEQUENCE_OFFSET, 0);
            writeLong(channel, READ_SEQUENCE_OFFSET, 0);
            writeInt(channel, WRITER_CLOSED_OFFSET, 0);
            channel.force(true);
            return new FileRing(normalized, channel, capacity, maxPayload);
        } catch (IOException | RuntimeException error) {
            channel.close();
            java.nio.file.Files.deleteIfExists(normalized);
            throw error;
        }
    }

    public static FileRing open(Path path) throws IOException {
        if (path == null) throw new IllegalArgumentException("path is required");
        Path normalized = path.toAbsolutePath().normalize();
        FileChannel channel = FileChannel.open(normalized, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            int magic = readInt(channel, MAGIC_OFFSET);
            int version = readInt(channel, VERSION_OFFSET);
            int capacity = readInt(channel, CAPACITY_OFFSET);
            int maxPayload = readInt(channel, MAX_PAYLOAD_OFFSET);
            if (magic != MAGIC || version != VERSION || capacity < 2 || maxPayload < 1) {
                throw new IOException("invalid Java ring header or unsupported version");
            }
            long required = HEADER_BYTES + (long) capacity * (Integer.BYTES + (long) maxPayload);
            if (required > Integer.MAX_VALUE || channel.size() != required) {
                throw new IOException("shared ring file size does not match its declared layout");
            }
            return new FileRing(normalized, channel, capacity, maxPayload);
        } catch (IOException | RuntimeException error) {
            channel.close();
            throw error;
        }
    }

    public int capacity() { return capacity; }
    public int maxPayloadBytes() { return maxPayload; }

    public Status send(byte[] payload, Duration timeout) throws IOException, InterruptedException {
        if (payload == null) return Status.INVALID_ARGUMENT;
        if (payload.length > maxPayload) return Status.TOO_LARGE;
        long deadline = deadline(timeout);
        int stride = Integer.BYTES + maxPayload;
        while (true) {
            try (Guard guard = lockUntil(deadline)) {
                if (guard == null) return Status.TIMEOUT;
                Header header = readHeader();
                if (header == null) return Status.INVALID_FRAME;
                if (header.closed) return Status.CLOSED;
                if (header.writeSequence - header.readSequence < capacity) {
                    int slot = (int) (header.writeSequence % capacity);
                    long offset = HEADER_BYTES + (long) slot * stride;
                    writeInt(channel, offset, payload.length);
                    writeFully(channel, ByteBuffer.wrap(payload), offset + Integer.BYTES);
                    writeLong(channel, WRITE_SEQUENCE_OFFSET, header.writeSequence + 1);
                    return Status.OK;
                }
            }
            if (!pauseUntil(deadline)) return Status.TIMEOUT;
        }
    }

    public Receive receive(Duration timeout) throws IOException, InterruptedException {
        long deadline = deadline(timeout);
        int stride = Integer.BYTES + maxPayload;
        while (true) {
            try (Guard guard = lockUntil(deadline)) {
                if (guard == null) return new Receive(Status.TIMEOUT, new byte[0]);
                Header header = readHeader();
                if (header == null) return new Receive(Status.INVALID_FRAME, new byte[0]);
                if (header.readSequence < header.writeSequence) {
                    int slot = (int) (header.readSequence % capacity);
                    long offset = HEADER_BYTES + (long) slot * stride;
                    int length = readInt(channel, offset);
                    if (length < 0 || length > maxPayload) {
                        return new Receive(Status.INVALID_FRAME, new byte[0]);
                    }
                    byte[] payload = new byte[length];
                    readFully(channel, ByteBuffer.wrap(payload), offset + Integer.BYTES);
                    writeLong(channel, READ_SEQUENCE_OFFSET, header.readSequence + 1);
                    return new Receive(Status.OK, payload);
                }
                if (header.closed) return new Receive(Status.END_OF_STREAM, new byte[0]);
            }
            if (!pauseUntil(deadline)) return new Receive(Status.TIMEOUT, new byte[0]);
        }
    }

    public Status closeWriter(Duration timeout) throws IOException, InterruptedException {
        long deadline = deadline(timeout);
        try (Guard guard = lockUntil(deadline)) {
            if (guard == null) return Status.TIMEOUT;
            if (readHeader() == null) return Status.INVALID_FRAME;
            writeInt(channel, WRITER_CLOSED_OFFSET, 1);
            return Status.OK;
        }
    }

    private Header readHeader() throws IOException {
        if (readInt(channel, MAGIC_OFFSET) != MAGIC || readInt(channel, VERSION_OFFSET) != VERSION ||
                readInt(channel, CAPACITY_OFFSET) != capacity ||
                readInt(channel, MAX_PAYLOAD_OFFSET) != maxPayload) return null;
        long write = readLong(channel, WRITE_SEQUENCE_OFFSET);
        long read = readLong(channel, READ_SEQUENCE_OFFSET);
        int closed = readInt(channel, WRITER_CLOSED_OFFSET);
        if (write < read || write - read > capacity || (closed != 0 && closed != 1)) return null;
        return new Header(write, read, closed == 1);
    }

    private Guard lockUntil(long deadline) throws IOException, InterruptedException {
        while (true) {
            long remaining = deadline - System.nanoTime();
            boolean localAcquired = remaining <= 0
                    ? localLock.tryLock()
                    : localLock.tryLock(remaining, TimeUnit.NANOSECONDS);
            if (!localAcquired) return null;
            try {
                FileLock lock = channel.tryLock(0, 1, false);
                if (lock != null) return new Guard(lock, localLock);
            } catch (OverlappingFileLockException ignored) {
                // Another channel in this JVM owns the file lock; retry to the same deadline.
            } catch (IOException | RuntimeException error) {
                localLock.unlock();
                throw error;
            }
            localLock.unlock();
            if (!pauseUntil(deadline)) return null;
        }
    }

    private static boolean pauseUntil(long deadline) throws InterruptedException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) return false;
        TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
        return deadline - System.nanoTime() > 0;
    }

    private static long deadline(Duration timeout) {
        if (timeout == null || timeout.isNegative()) throw new IllegalArgumentException("timeout must be nonnegative");
        long nanos = timeout.toNanos();
        long now = System.nanoTime();
        return nanos > 0 && now > Long.MAX_VALUE - nanos ? Long.MAX_VALUE : now + nanos;
    }

    private record Header(long writeSequence, long readSequence, boolean closed) { }

    private static final class Guard implements AutoCloseable {
        private final FileLock fileLock;
        private final ReentrantLock localLock;
        private Guard(FileLock fileLock, ReentrantLock localLock) {
            this.fileLock = fileLock;
            this.localLock = localLock;
        }
        @Override public void close() throws IOException {
            try { fileLock.release(); } finally { localLock.unlock(); }
        }
    }

    @Override public void close() throws IOException {
        channel.close();
    }

    private static int readInt(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        readFully(channel, buffer, position);
        buffer.flip();
        return buffer.getInt();
    }

    private static long readLong(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        readFully(channel, buffer, position);
        buffer.flip();
        return buffer.getLong();
    }

    private static void writeInt(FileChannel channel, long position, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
        buffer.flip();
        writeFully(channel, buffer, position);
    }

    private static void writeLong(FileChannel channel, long position, long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value);
        buffer.flip();
        writeFully(channel, buffer, position);
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int count = channel.read(buffer, position);
            if (count < 0) throw new IOException("unexpected end of session file");
            if (count == 0) Thread.onSpinWait();
            position += count;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int count = channel.write(buffer, position);
            if (count == 0) Thread.onSpinWait();
            position += count;
        }
    }
}
