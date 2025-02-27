package io.nexus.streamlets.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class SerializationUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo(new DefaultClassResolver(), new MapReferenceResolver());
        kryo.setRegistrationRequired(false); // Allow dynamic registration
        return kryo;
    });

    public static <T> byte[] kryoSerialize(T value) {
        Kryo kryo = kryoThreadLocal.get();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (Output output = new Output(bos)) {
            kryo.writeObject(output, value);
            output.flush();
            return bos.toByteArray();
        }
    }

    public static <T> T kryoDeserialize(byte[] data, Class<T> type) {
        Kryo kryo = kryoThreadLocal.get();
        try (Input input = new Input(data)) {
            return kryo.readObject(input, type);
        }
    }

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

