package io.nexus.streamlets.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class SerializationUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> String serialize(T value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public static <T> T deserialize(String data, Class<T> type) {
        try {
            return objectMapper.readValue(data, type);
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }

    // TODO: Rework this so we can get an object of a specified type
    private static void deserializeRecords(byte[] target, int currentBytesRead) {
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

