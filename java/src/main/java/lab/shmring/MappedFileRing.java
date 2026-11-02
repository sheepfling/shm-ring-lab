package lab.shmring;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Shared mapped-file SPSC ring. The file layout is private to this Java implementation. */
public final class MappedFileRing implements AutoCloseable {
    public enum Status { OK, TIMEOUT, TOO_LARGE, CLOSED, END_OF_STREAM, INVALID_ARGUMENT, INVALID_FRAME }

    public static final class Receive {
        public final Status status;
        public final byte[] payload;
        private Receive(Status status, byte[] payload) {
            this.status = status;
            this.payload = payload;
        }
    }

    private static final int MAGIC = 0x4a4d5231; // JMR1, distinct from the FileRing layout
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 64;
    private static final int WRITE_SEQUENCE = 16;
    private static final int READ_SEQUENCE = 24;
    private static final int WRITER_CLOSED = 32;
    private static final ConcurrentHashMap<Path, ReentrantLock> LOCAL_LOCKS = new ConcurrentHashMap<>();

    private final Path path;
    private final FileChannel channel;
    private final ReentrantLock localLock;
    private final int capacity;
    private final int maxPayload;
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock(true);
    private MappedByteBuffer mapping;

    private MappedFileRing(Path path, FileChannel channel, MappedByteBuffer mapping,
                           int capacity, int maxPayload) {
        this.path = path.toAbsolutePath().normalize();
        this.channel = channel;
        mapping.order(ByteOrder.LITTLE_ENDIAN);
        this.mapping = mapping;
        this.capacity = capacity;
        this.maxPayload = maxPayload;
        this.localLock = LOCAL_LOCKS.computeIfAbsent(this.path, ignored -> new ReentrantLock());
    }

    public static MappedFileRing create(Path path, int capacity, int maxPayload) throws IOException {
        if (path == null || capacity < 2 || maxPayload < 1) {
            throw new IllegalArgumentException("path, capacity >= 2, and maxPayload >= 1 are required");
        }
        long byteCount = HEADER_BYTES + (long) capacity * (Integer.BYTES + (long) maxPayload);
        if (byteCount > Integer.MAX_VALUE) throw new IllegalArgumentException("mapped session exceeds 2 GiB");
        Path normalized = path.toAbsolutePath().normalize();
        FileChannel channel = createChannel(normalized);
        try {
            channel.position(byteCount - 1);
            channel.write(ByteBuffer.wrap(new byte[] {0}));
            MappedByteBuffer mapping = channel.map(FileChannel.MapMode.READ_WRITE, 0, byteCount);
            mapping.order(ByteOrder.LITTLE_ENDIAN);
            mapping.putInt(0, MAGIC);
            mapping.putInt(4, VERSION);
            mapping.putInt(8, capacity);
            mapping.putInt(12, maxPayload);
            mapping.putLong(WRITE_SEQUENCE, 0);
            mapping.putLong(READ_SEQUENCE, 0);
            mapping.putInt(WRITER_CLOSED, 0);
            return new MappedFileRing(normalized, channel, mapping, capacity, maxPayload);
        } catch (IOException | RuntimeException error) {
            channel.close();
            Files.deleteIfExists(normalized);
            throw error;
        }
    }

    public static MappedFileRing open(Path path) throws IOException {
        if (path == null) throw new IllegalArgumentException("path is required");
        Path normalized = path.toAbsolutePath().normalize();
        FileChannel channel = FileChannel.open(normalized, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            long byteCount = channel.size();
            if (byteCount < HEADER_BYTES || byteCount > Integer.MAX_VALUE) {
                throw new IOException("mapped session size is outside the supported range");
            }
            MappedByteBuffer mapping = channel.map(FileChannel.MapMode.READ_WRITE, 0, byteCount);
            mapping.order(ByteOrder.LITTLE_ENDIAN);
            // Read only the header pages needed for validation. load() is a paging
            // hint with potentially mapping-wide I/O cost, not a visibility barrier.
            int capacity = mapping.getInt(8);
            int maxPayload = mapping.getInt(12);
            long expectedBytes = HEADER_BYTES + (long) capacity * (Integer.BYTES + (long) maxPayload);
            if (mapping.getInt(0) != MAGIC || mapping.getInt(4) != VERSION || capacity < 2 ||
                    maxPayload < 1 || expectedBytes != byteCount) {
                throw new IOException("invalid mapped-ring header, layout, or version");
            }
            return new MappedFileRing(normalized, channel, mapping, capacity, maxPayload);
        } catch (IOException | RuntimeException error) {
            channel.close();
            throw error;
        }
    }

    public int capacity() { return capacity; }
    public int maxPayloadBytes() { return maxPayload; }

    public Status send(byte[] payload, Duration timeout) throws IOException, InterruptedException {
        lifecycle.readLock().lock();
        try {
            currentMapping();
            return sendWhileOpen(payload, timeout);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private Status sendWhileOpen(byte[] payload, Duration timeout) throws IOException, InterruptedException {
        if (payload == null) return Status.INVALID_ARGUMENT;
        if (payload.length > maxPayload) return Status.TOO_LARGE;
        long deadline = deadline(timeout);
        int stride = Integer.BYTES + maxPayload;
        while (true) {
            try (Guard guard = lockUntil(deadline)) {
                if (guard == null) return Status.TIMEOUT;
                MappedByteBuffer map = currentMapping();
                Header header = readHeader(map);
                if (header == null) return Status.INVALID_FRAME;
                if (header.closed) return Status.CLOSED;
                if (header.write - header.read < capacity) {
                    int slot = (int) (header.write % capacity);
                    int offset = HEADER_BYTES + slot * stride;
                    map.putInt(offset, payload.length);
                    for (int i = 0; i < payload.length; i++) map.put(offset + Integer.BYTES + i, payload[i]);
                    map.putLong(WRITE_SEQUENCE, header.write + 1);
                    return Status.OK;
                }
            }
            if (!pauseUntil(deadline)) return Status.TIMEOUT;
        }
    }

    public Receive receive(Duration timeout) throws IOException, InterruptedException {
        lifecycle.readLock().lock();
        try {
            currentMapping();
            return receiveWhileOpen(timeout);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private Receive receiveWhileOpen(Duration timeout) throws IOException, InterruptedException {
        long deadline = deadline(timeout);
        int stride = Integer.BYTES + maxPayload;
        while (true) {
            try (Guard guard = lockUntil(deadline)) {
                if (guard == null) return new Receive(Status.TIMEOUT, new byte[0]);
                MappedByteBuffer map = currentMapping();
                Header header = readHeader(map);
                if (header == null) return new Receive(Status.INVALID_FRAME, new byte[0]);
                if (header.read < header.write) {
                    int slot = (int) (header.read % capacity);
                    int offset = HEADER_BYTES + slot * stride;
                    int length = map.getInt(offset);
                    if (length < 0 || length > maxPayload) return new Receive(Status.INVALID_FRAME, new byte[0]);
                    byte[] payload = new byte[length];
                    for (int i = 0; i < length; i++) payload[i] = map.get(offset + Integer.BYTES + i);
                    map.putLong(READ_SEQUENCE, header.read + 1);
                    return new Receive(Status.OK, payload);
                }
                if (header.closed) return new Receive(Status.END_OF_STREAM, new byte[0]);
            }
            if (!pauseUntil(deadline)) return new Receive(Status.TIMEOUT, new byte[0]);
        }
    }

    public Status closeWriter(Duration timeout) throws IOException, InterruptedException {
        lifecycle.readLock().lock();
        try {
            currentMapping();
            return closeWriterWhileOpen(timeout);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private Status closeWriterWhileOpen(Duration timeout) throws IOException, InterruptedException {
        long deadline = deadline(timeout);
        try (Guard guard = lockUntil(deadline)) {
            if (guard == null) return Status.TIMEOUT;
            MappedByteBuffer map = currentMapping();
            if (readHeader(map) == null) return Status.INVALID_FRAME;
            map.putInt(WRITER_CLOSED, 1);
            return Status.OK;
        }
    }

    private MappedByteBuffer currentMapping() throws IOException {
        if (mapping == null || !channel.isOpen()) throw new IOException("mapped session is closed");
        return mapping;
    }

    private static FileChannel createChannel(Path path) throws IOException {
        if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            Files.createFile(path, PosixFilePermissions.asFileAttribute(
                    EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)));
            try {
                return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException error) {
                Files.deleteIfExists(path);
                throw error;
            }
        }
        return FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private Header readHeader(ByteBuffer map) {
        if (map.getInt(0) != MAGIC || map.getInt(4) != VERSION ||
                map.getInt(8) != capacity || map.getInt(12) != maxPayload) return null;
        long write = map.getLong(WRITE_SEQUENCE);
        long read = map.getLong(READ_SEQUENCE);
        int closed = map.getInt(WRITER_CLOSED);
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
                // This is cooperative interprocess serialization. Java does not define
                // a mapped-buffer happens-before edge across JVMs; supported filesystems
                // must provide coherent shared mappings and honor this native file lock.
                FileLock lock = channel.tryLock(0, 1, false);
                if (lock != null) return new Guard(lock, localLock);
            } catch (OverlappingFileLockException ignored) {
                // Serialize channels in one JVM with localLock; this covers unrelated aliases too.
            } catch (IOException | RuntimeException error) {
                localLock.unlock();
                throw error;
            }
            localLock.unlock();
            if (!pauseUntil(deadline)) return null;
        }
    }

    private static long deadline(Duration timeout) {
        if (timeout == null || timeout.isNegative()) throw new IllegalArgumentException("timeout must be nonnegative");
        long nanos = timeout.toNanos();
        long now = System.nanoTime();
        return nanos > 0 && now > Long.MAX_VALUE - nanos ? Long.MAX_VALUE : now + nanos;
    }

    private static boolean pauseUntil(long deadline) throws InterruptedException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) return false;
        TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
        return deadline - System.nanoTime() > 0;
    }

    private record Header(long write, long read, boolean closed) { }

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
        lifecycle.writeLock().lock();
        try {
            MappedByteBuffer map = mapping;
            if (map == null) return;
            // File locks are held on behalf of the JVM. Serialize channel close against
            // critical sections from every same-path ring in this JVM before closing.
            localLock.lock();
            try {
                mapping = null;
                channel.close();
            } finally {
                localLock.unlock();
            }
            // Java 17 has no supported deterministic unmap API; delete only after all
            // participating JVMs have exited, especially on Windows.
        } finally {
            lifecycle.writeLock().unlock();
        }
    }
}
