package io.nexus.streamlets.functions;

import io.nexus.streamlets.TransformerStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.nexus.streamlets.utils.InputStreamRecord;
import io.pravega.common.util.ByteArraySegment;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;

/**
 * 
 * A basic Streamlet that simply reads the provided data
 * 
 */

public class NoOpStreamlet extends TransformerStreamlet {

    private final String name;

    public NoOpStreamlet(String name) {
        this.name = name;
    }

    @Override
    public void handlePut(InputStreamRecord event, StreamletContext context) {
        Logger logger = context.getLogger();
        Policy policy = context.getPolicy();

        // Example of adding metadata
        context.putUserMetadata("encryption", "lz4");

        logger.info("PUT - Executing Streamlet: " + name + ", as part of pipeline: {}", policy.getPipeline());
        doRead(event.input(), event.output(), logger);
    }

    @Override
    public void handleGet(InputStreamRecord event, StreamletContext context) {
        Logger logger = context.getLogger();
        Policy policy = context.getPolicy();

        // Example of getting metadata
        String encryptionType = context.getUserMetadata("encryption");
        logger.info("User Metadata - Encryption type: {}", encryptionType);

        logger.info("GET - Executing Streamlet: " + name + ", as part of pipeline: {}", policy.getPipeline());
        doRead(event.input(), event.output(), logger);
    }

    private void doRead(ByteBufferPipelineStream input, ByteBufferPipelineStream output, Logger logger) {
        // TODO: Move this logic into ByteBufferPipelineStream
        int totalBytesRead = 0;

        try {
            int currentBytesRead = 0;
            while (currentBytesRead != -1) {
                // TODO: Adjust array size to be compliant with streaming services conventions
                byte[] target = new byte[8192];
                currentBytesRead = input.read(target);
                if (currentBytesRead > 0) {
                    ByteArraySegment readData = new ByteArraySegment(target, 0, currentBytesRead);
                    output.addSegment(readData);
                    totalBytesRead += currentBytesRead;

                    // TODO: This function will be useful during the serialization work
                    // deserializeRecords(target, currentBytesRead, logger);

                }
                logger.info("Finished Streamlet " + name + " operations. Processed Bytes: " + totalBytesRead);
                output.close();
            }
        } catch (Exception e) {
            logger.error("Error deserializing the input", e);
        }
    }

    private static void deserializeRecords(byte[] target, int currentBytesRead, Logger logger) {
        // Deserialize the read data
        ByteBuffer buffer = ByteBuffer.wrap(target, 0, currentBytesRead);
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                long offset = record.offset();
                String key = byteBufferToString(record.key());
                String value = byteBufferToString(record.value());
                long timestamp = record.timestamp();
                int keySize = record.keySize();
                int valueSize = record.valueSize();

                logger.info("---Current Record---");
                logger.info("Record offset: " + offset);
                logger.info("Timestamp: " + timestamp);
                logger.info("Key size: " + keySize + ", key: " + key);
                logger.info("Value size: " + valueSize + ", value: " + value);
            }
        }
    }

    private static String byteBufferToString(ByteBuffer buffer) {
        if (buffer == null) {
            return "null";
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.slice().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
