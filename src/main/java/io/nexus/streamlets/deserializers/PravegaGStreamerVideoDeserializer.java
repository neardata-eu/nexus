package io.nexus.streamlets.deserializers;

import io.nexus.streamlets.DeserializationResult;
import io.nexus.streamlets.Deserializer;
import io.nexus.streamlets.utils.PravegaGStreamerVideoFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Deserializer for video frames written by Pravega GStreamer connector.
 *
 * Frame format: [4 bytes buffer size][8 bytes timestamp][buffer data]
 * - Buffer size: 32-bit integer (little-endian)
 * - Timestamp: 64-bit long, nanoseconds since 1970-01-01 00:00:00 TAI (little-endian)
 * - Buffer data: raw video frame bytes
 */
public class PravegaGStreamerVideoDeserializer implements Deserializer<PravegaGStreamerVideoFrame> {
    private final Logger logger = LoggerFactory.getLogger(PravegaGStreamerVideoDeserializer.class);

    // Frame header size: 4 bytes (buffer size) + 8 bytes (timestamp)
    private static final int FRAME_HEADER_SIZE = 12;

    @Override
    public DeserializationResult<PravegaGStreamerVideoFrame> deserializeChunk(InputStream input) throws IOException {
        byte[] data = input.readAllBytes();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // GStreamer typically uses little-endian

        List<PravegaGStreamerVideoFrame> frames = new ArrayList<>();
        int totalBytesConsumed = 0;

        try {
            while (buffer.remaining() >= FRAME_HEADER_SIZE) {
                int startPosition = buffer.position();

                // Read frame header
                int frameSize = buffer.getInt();
                long timestamp = buffer.getLong();

                // Validate frame size
                if (frameSize < 0 || frameSize > buffer.remaining()) {
                    logger.warn("Invalid frame size: {}. Remaining bytes: {}. Skipping rest of chunk.",
                            frameSize, buffer.remaining());
                    break;
                }

                // Read frame data
                byte[] frameData = new byte[frameSize];
                buffer.get(frameData);

                // Create video frame object
                PravegaGStreamerVideoFrame frame = new PravegaGStreamerVideoFrame(frameData, timestamp, frameSize);
                frames.add(frame);

                // Calculate bytes consumed for this frame (header + data)
                int frameBytesConsumed = FRAME_HEADER_SIZE + frameSize;
                totalBytesConsumed += frameBytesConsumed;

                logger.debug("Deserialized frame: size={} bytes, timestamp={} ns, total_consumed={}",
                        frameSize, timestamp, totalBytesConsumed);
            }

            if (buffer.hasRemaining()) {
                logger.debug("Incomplete frame data remaining: {} bytes", buffer.remaining());
            }

        } catch (Exception e) {
            logger.error("Error deserializing Pravega video frames: {}", e.getMessage(), e);
        }

        logger.info("Successfully deserialized {} video frames, consumed {} bytes",
                frames.size(), totalBytesConsumed);

        return new DeserializationResult<>(frames, totalBytesConsumed);
    }

    /**
     * Alternative method to deserialize a single frame from a byte array
     * Useful for testing or when working with individual frame data
     */
    public PravegaGStreamerVideoFrame deserializeSingleFrame(byte[] frameBytes) {
        if (frameBytes.length < FRAME_HEADER_SIZE) {
            throw new IllegalArgumentException("Frame data too small. Expected at least " +
                    FRAME_HEADER_SIZE + " bytes, got " + frameBytes.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(frameBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int frameSize = buffer.getInt();
        long timestamp = buffer.getLong();

        if (frameSize != frameBytes.length - FRAME_HEADER_SIZE) {
            logger.warn("Frame size mismatch. Header indicates: {}, actual data: {}",
                    frameSize, frameBytes.length - FRAME_HEADER_SIZE);
        }

        byte[] frameData = new byte[frameBytes.length - FRAME_HEADER_SIZE];
        buffer.get(frameData);

        return new PravegaGStreamerVideoFrame(frameData, timestamp, frameSize);
    }

    /**
     * Utility method to convert TAI timestamp to Unix timestamp (approximate)
     * Note: This is approximate as it doesn't account for leap seconds
     */
    public static long taiToUnixTimestamp(long taiNanos) {
        // TAI is approximately 37 seconds ahead of UTC (as of 2024)
        // This is an approximation and may need adjustment based on current leap seconds
        final long TAI_UTC_OFFSET_NANOS = 37_000_000_000L; // 37 seconds in nanoseconds
        return taiNanos - TAI_UTC_OFFSET_NANOS;
    }
}

