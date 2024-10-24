package io.nexus.streamlets.utils;
import io.pravega.common.util.ByteArraySegment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ByteBufferPipelineStream extends InputStream {
    // Queue to hold ByteArraySegments
    private final Queue<ByteArraySegment> segmentQueue = new ConcurrentLinkedQueue<>();

    // Current ByteArraySegment being read
    private ByteArraySegment currentSegment = null;

    // Position within the current ByteArraySegment
    private int currentPosition = 0;

    // Flag to indicate if the stream is closed
    private boolean isClosed = false;

    @Override
    public synchronized int read() throws IOException {
        while (!this.isClosed || !this.segmentQueue.isEmpty()) {
            // Ensure that the current segment is ready to be read from
            if (prepareCurrentSegment()) {
                int byteValue = this.currentSegment.get(this.currentPosition) & 0xFF;
                this.currentPosition++;
                return byteValue;
            }

            // Wait for more data to be added if the stream is not closed
            waitForData();
        }

        return -1;  // End of stream
    }

    @Override
    public synchronized int read(byte[] recipientArray, int recipientOffset, int recipientLength) throws IOException {
        if (recipientArray == null) {
            throw new NullPointerException();
        } else if (recipientOffset < 0 || recipientLength < 0 || recipientLength > recipientArray.length - recipientOffset) {
            throw new IndexOutOfBoundsException();
        } else if (recipientLength == 0) {
            return 0;
        }

        int bytesRead = 0;
        while (recipientLength > 0 && (!this.isClosed || !this.segmentQueue.isEmpty() || this.currentSegment != null)) {
            if (prepareCurrentSegment()) {
                int available = this.currentSegment.getLength() - this.currentPosition;
                int toRead = Math.min(recipientLength, available);
                // TODO: Check how costly is this.
                System.arraycopy(this.currentSegment.array(), this.currentSegment.arrayOffset() + this.currentPosition,
                        recipientArray, recipientOffset + bytesRead, toRead);
                this.currentPosition += toRead;
                bytesRead += toRead;
                recipientLength -= toRead;
            } else {
                // Wait for more data to be added if the stream is not closed
                waitForData();
            }
        }
        return bytesRead > 0 ? bytesRead : -1;
    }

    @Override
    public synchronized int available() throws IOException {
        if (this.currentSegment != null) {
            return this.currentSegment.getLength() - this.currentPosition;
        }

        int available = 0;
        for (ByteArraySegment segment : this.segmentQueue) {
            available += segment.getLength();
        }

        return available;
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        long skipped = 0;

        while (n > 0 && (!this.isClosed || !this.segmentQueue.isEmpty())) {
            if (prepareCurrentSegment()) {
                int available = this.currentSegment.getLength() - this.currentPosition;
                long toSkip = Math.min(n, available);

                this.currentPosition += toSkip;
                skipped += toSkip;
                n -= toSkip;
            } else {
                // Wait for more data to be added if the stream is not closed
                waitForData();
            }
        }

        return skipped;
    }

    @Override
    public synchronized void close() throws IOException {
        this.isClosed = true;
        notifyAll();  // Wake up any waiting threads
    }

    /**
     * Adds a new ByteArraySegment to the queue. This can be done while the InputStream is being read.
     */
    public synchronized void addSegment(ByteArraySegment segment) {
        if (segment != null && segment.getLength() > 0) {
            this.segmentQueue.add(segment);
            notifyAll();  // Wake up any waiting threads
        }
    }

    /**
     * Prepares the current segment for reading.
     *
     * @return true if the current segment is ready to be read, false if no more data is available.
     */
    private boolean prepareCurrentSegment() {
        if (this.currentSegment == null || this.currentPosition >= this.currentSegment.getLength()) {
            this.currentSegment = this.segmentQueue.poll();
            this.currentPosition = 0;
        }
        return this.currentSegment != null;
    }

    /**
     * Waits for more data to be added if the stream is not closed.
     */
    private void waitForData() {
        try {
            while (this.segmentQueue.isEmpty() && !this.isClosed) {
                wait(); // Wait until notifyAll() is called on adding a segment or closing the stream
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while waiting for data.", e);
        }
    }
}