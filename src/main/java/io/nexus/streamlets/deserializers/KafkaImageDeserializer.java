package io.nexus.streamlets.deserializers;

import io.nexus.streamlets.DeserializationResult;
import io.nexus.streamlets.Deserializer;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KafkaImageDeserializer implements Deserializer<byte[]> {

    @Override
    public DeserializationResult<byte[]> deserializeChunk(InputStream input) throws IOException {
        byte[] data = input.readAllBytes(); // full accumulated chunk
        ByteBuffer buffer = ByteBuffer.wrap(data);
        List<byte[]> result = new ArrayList<>();
        int bytesConsumed = 0;
        try {
            MemoryRecords records = MemoryRecords.readableRecords(buffer);
            for (RecordBatch batch : records.batches()) {
                bytesConsumed += batch.sizeInBytes(); // count full batch size
                for (Record record : batch) {
                    ByteBuffer value = record.value();
                    if (value != null) {
                        byte[] imageBytes = new byte[value.remaining()];
                        value.get(imageBytes);
                        result.add(imageBytes);
                    }
                }
            }
        } catch (CorruptRecordException | BufferUnderflowException e) {
            // likely a partial batch: ignore and return what we have
        }
        return new DeserializationResult<>(result, bytesConsumed);
    }
}