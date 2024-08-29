package io.nexus.streamlets.utils;

import io.pravega.common.util.ByteArraySegment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamicInputStream extends InputStream {
    private final Queue<ByteArraySegment> bufferQueue = new ConcurrentLinkedQueue<>();
    private ByteArraySegment currentSegment = null;
    private int currentIndex = 0;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public synchronized int read() throws IOException {
        while (true) {
            if (currentSegment == null || currentIndex >= currentSegment.getLength()) {
                currentSegment = bufferQueue.poll();
                if (currentSegment == null) {
                    if (closed.get()) {
                        return -1; // End of stream
                    }
                    try {
                        wait(); // Wait for data to be available
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Thread interrupted while reading", e);
                    }
                    continue;
                }
                currentIndex = 0;
            }

            return currentSegment.get(currentIndex++) & 0xFF;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed.set(true);
        notifyAll(); // Wake up any waiting threads
    }

    public synchronized void addSegment(ByteArraySegment segment) {
        if (closed.get()) {
            throw new IllegalStateException("Stream is closed. Cannot add more data.");
        }
        bufferQueue.add(segment);
        notifyAll(); // Notify any waiting threads that new data is available
    }

    public boolean isClosed() {
        return closed.get();
    }
}