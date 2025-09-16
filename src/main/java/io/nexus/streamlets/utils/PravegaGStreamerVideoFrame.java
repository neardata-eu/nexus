package io.nexus.streamlets.utils;

/**
 * Pravega GStreamer video frame data structure containing frame bytes and timestamp.
 */
public class PravegaGStreamerVideoFrame {
    private final byte[] frameData;
    private final long timestamp; // nanoseconds since 1970-01-01 00:00:00 TAI
    private final int frameSize;

    public PravegaGStreamerVideoFrame(byte[] frameData, long timestamp, int frameSize) {
        this.frameData = frameData;
        this.timestamp = timestamp;
        this.frameSize = frameSize;
    }

    public byte[] getFrameData() { return frameData; }
    public long getTimestamp() { return timestamp; }
    public int getFrameSize() { return frameSize; }

    @Override
    public String toString() {
        return String.format("PravegaGStreamerVideoFrame{size=%d, timestamp=%d ns}", frameSize, timestamp);
    }
}