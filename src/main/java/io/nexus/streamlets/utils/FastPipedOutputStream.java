package io.nexus.streamlets.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A high-performance piped output stream that writes to a circular buffer.
 */
public class FastPipedOutputStream extends OutputStream {
    private static final int DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;
    private static final long READ_TIMEOUT_MS = 5000; // 5 seconds timeout

    private final byte[] buffer;
    private int writePos = 0;
    private int readPos = 0;
    private int available = 0;
    private boolean closed = false;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public FastPipedOutputStream() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public FastPipedOutputStream(int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be greater than 0");
        }
        this.buffer = new byte[bufferSize];
    }

    protected void connect(FastPipedInputStream inputStream) {
        inputStream.setSource(this);
    }

    @Override
    public void write(int b) throws IOException {
        lock.lock();
        try {
            while (available == buffer.length) {
                if (closed) {
                    throw new IOException("Stream is closed");
                }
                notFull.await();
            }
            buffer[writePos] = (byte) b;
            writePos = (writePos + 1) % buffer.length;
            available++;
            notEmpty.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        lock.lock();
        try {
            for (int i = 0; i < len; i++) {
                while (available == buffer.length) {
                    if (closed) {
                        throw new IOException("Stream is closed");
                    }
                    notFull.await();
                }
                buffer[writePos] = b[off + i];
                writePos = (writePos + 1) % buffer.length;
                available++;
                notEmpty.signal();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing", e);
        } finally {
            lock.unlock();
        }
    }

    protected int read() throws IOException {
        lock.lock();
        try {
            while (available == 0) {
                if (closed) return -1;
                notEmpty.await();
            }
            int data = buffer[readPos] & 0xFF;
            readPos = (readPos + 1) % buffer.length;
            available--;
            notFull.signal();
            return data;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading", e);
        } finally {
            lock.unlock();
        }
    }

    protected int read(byte[] b, int off, int len) throws IOException {
        lock.lock();
        try {
            while (true) {
                if (available > 0) {
                    int toRead = Math.min(len, available);
                    for (int i = 0; i < toRead; i++) {
                        b[off + i] = buffer[readPos];
                        readPos = (readPos + 1) % buffer.length;
                    }
                    available -= toRead;
                    notFull.signal();
                    return toRead;
                } else if (closed) {
                    return -1;
                } else if (!notEmpty.await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    throw new IOException("Read timeout exceeded");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();

        try {
            if (!closed) {
                closed = true;
                notEmpty.signalAll(); // Wake up waiting readers
                notFull.signalAll();  // Wake up waiting writers
            }
        } finally {
            lock.unlock();
        }
    }
}
