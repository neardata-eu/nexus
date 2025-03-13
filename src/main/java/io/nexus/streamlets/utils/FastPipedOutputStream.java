package io.nexus.streamlets.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

/**
 * A high-performance piped output stream that writes to a circular buffer with reduced contention. This class is
 * designed for inter-thread communication where one thread writes data while another reads it through a connected
 * {@link FastPipedInputStream}.
 */
public class FastPipedOutputStream extends OutputStream {

    private static final int DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; // 2 MB default buffer size

    private final byte[] buffer;
    private int writePos = 0;
    private int readPos = 0;
    private int available = 0;
    private boolean closed = false;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    /**
     * Creates a new {@code FastPipedOutputStream} with the default buffer size.
     */
    public FastPipedOutputStream() {
        this(DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new {@code FastPipedOutputStream} with a specified buffer size.
     *
     * @param bufferSize the size of the internal buffer
     * @throws IllegalArgumentException if the buffer size is less than or equal to zero
     */
    public FastPipedOutputStream(int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be greater than 0");
        }
        this.buffer = new byte[bufferSize];
    }

    /**
     * Connects this output stream to a {@link FastPipedInputStream}.
     * This method is used internally when creating a piped stream pair.
     *
     * @param inputStream the input stream to connect to
     */
    protected void connect(FastPipedInputStream inputStream) {
        inputStream.setSource(this);
    }

    /**
     * Writes a single byte to the output stream.
     * Blocks if the buffer is full until space becomes available.
     *
     * @param b the byte to write
     * @throws IOException if the stream is closed or the thread is interrupted
     */
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
            notEmpty.signal(); // Notify a waiting reader
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes an array of bytes to the output stream.
     * Attempts to write as much as possible in a single operation to improve performance.
     *
     * @param b   the byte array to write
     * @param off the offset in the array
     * @param len the number of bytes to write
     * @throws IOException if the stream is closed or the thread is interrupted
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        lock.lock();
        try {
            int written = 0;
            while (written < len) {
                while (available == buffer.length) {
                    if (closed) {
                        throw new IOException("Stream is closed");
                    }
                    notFull.await();
                }

                int space = buffer.length - available;
                int toWrite = Math.min(len - written, space);
                int firstChunk = Math.min(toWrite, buffer.length - writePos);
                int secondChunk = toWrite - firstChunk;

                System.arraycopy(b, off + written, buffer, writePos, firstChunk);
                if (secondChunk > 0) {
                    System.arraycopy(b, off + written + firstChunk, buffer, 0, secondChunk);
                }

                writePos = (writePos + toWrite) % buffer.length;
                available += toWrite;
                written += toWrite;

                notEmpty.signal(); // Notify a waiting reader
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads a byte from the output stream buffer.
     * This method is called by the connected {@link FastPipedInputStream}.
     *
     * @return the next byte, or -1 if the stream is closed
     * @throws IOException if the thread is interrupted
     */
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
            notFull.signal(); // Notify a waiting writer
            return data;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads multiple bytes from the output stream buffer.
     * Attempts to read as much as possible in a single operation to improve performance.
     *
     * @param b   the buffer to store the read bytes
     * @param off the offset in the buffer
     * @param len the maximum number of bytes to read
     * @return the number of bytes read, or -1 if the stream is closed
     * @throws IOException if the thread is interrupted
     */
    protected int read(byte[] b, int off, int len) throws IOException {
        lock.lock();
        try {
            while (available == 0) {
                if (closed) return -1;
                notEmpty.await();
            }

            int toRead = Math.min(len, available);
            int firstChunk = Math.min(toRead, buffer.length - readPos);
            int secondChunk = toRead - firstChunk;

            System.arraycopy(buffer, readPos, b, off, firstChunk);
            if (secondChunk > 0) {
                System.arraycopy(buffer, 0, b, off + firstChunk, secondChunk);
            }

            readPos = (readPos + toRead) % buffer.length;
            available -= toRead;

            notFull.signal(); // Notify a waiting writer
            return toRead;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the output stream.
     * Notifies all waiting threads that the stream is closed.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                notEmpty.signalAll(); // Wake up readers
                notFull.signalAll();  // Wake up writers
            }
        } finally {
            lock.unlock();
        }
    }
}
