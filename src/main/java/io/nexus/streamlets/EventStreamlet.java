package io.nexus.streamlets;

import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static io.nexus.streamlets.StreamletsMetrics.GET_STREAMLET_EXECUTION_LATENCY_TIMER;
import static io.nexus.streamlets.StreamletsMetrics.PUT_STREAMLET_EXECUTION_LATENCY_TIMER;

/**
 * A Streamlet that processes events instead of raw bytes. Events are deserialized from the input stream and processed.
 * This class does not allow to write serialized events back to the output stream, as data in storage should be the same
 * as it was written by the streaming system (i.e., only lossless transformations are allowed). Otherwise, it may cause
 * data corruption.
 * Important: due to the dynamic loading capabilities of Streamlets, Nexus assumes that Streamlets extending this
 * class are instantiated via a constructor with one {@link Deserializer} argument.
 *
 * @param <T> The type of record being processed.
 */
public abstract class EventStreamlet<T> implements Streamlet {
    private final Logger logger = LoggerFactory.getLogger(EventStreamlet.class);
    private final static int READ_SIZE = 512 * 1024;
    private final Deserializer<T> deserializer;
    private final ByteArrayOutputStream leftoverBuffer = new ByteArrayOutputStream();

    /**
     * Constructs a RecordStreamlet with the given deserializer and serializer.
     *
     * @param deserializer The deserializer to convert input data into records.
     */
    public EventStreamlet(Deserializer<T> deserializer) {
        this.deserializer = deserializer;
    }

    /**
     * Processes a single record for PUT requests.
     *
     * @param record The input record.
     */
    protected abstract void processPutRecord(T record, StreamletContext context);

    /**
     * Processes a single record for GET requests.
     *
     * @param record The input record.
     */
    protected abstract void processGetRecord(T record, StreamletContext context);

    @Override
    public void handlePut(StreamletIO dataStreams, StreamletContext context) {
        long startTime = System.nanoTime();
        handleRequest(dataStreams, context, this::processPutRecord);
        PUT_STREAMLET_EXECUTION_LATENCY_TIMER.record(System.nanoTime() - startTime);
    }
    @Override
    public void handleGet(StreamletIO dataStreams, StreamletContext context) {
        long startTime = System.nanoTime();
        handleRequest(dataStreams, context, this::processGetRecord);
        GET_STREAMLET_EXECUTION_LATENCY_TIMER.record(System.nanoTime() - startTime);
    }

    private void handleRequest(StreamletIO streams, StreamletContext context, BiConsumer<T, StreamletContext> handler) {
        byte[] buffer = new byte[READ_SIZE];
        int bytesRead;
        try (InputStream input = streams.input();
             OutputStream output = streams.output()) {
            ByteArrayOutputStream leftoverBuffer = new ByteArrayOutputStream();
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
                leftoverBuffer.write(buffer, 0, bytesRead);
                byte[] combined = leftoverBuffer.toByteArray();
                try (ByteArrayInputStream chunkStream = new ByteArrayInputStream(combined)) {
                    DeserializationResult<T> result = deserializer.deserializeChunk(chunkStream);
                    for (T record : result.records()) {
                        handler.accept(record, context);
                    }
                    // Preserve only leftover unparsed bytes
                    byte[] leftoverBytes = Arrays.copyOfRange(combined, result.bytesConsumed(), combined.length);
                    leftoverBuffer.reset();
                    leftoverBuffer.write(leftoverBytes);
                } catch (IOException e) {
                    logger.error("Failed to deserialize chunk", e);
                } catch (Exception e) {
                    logger.error("Unexpected error during deserialization", e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Stream handling failed", e);
        }
    }
}

