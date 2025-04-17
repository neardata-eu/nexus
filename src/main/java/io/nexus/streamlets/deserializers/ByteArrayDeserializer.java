package io.nexus.streamlets.deserializers;

import io.nexus.streamlets.DeserializationResult;
import io.nexus.streamlets.Deserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteArrayDeserializer implements Deserializer<byte[]> {
    private final int chunkSize;

    public ByteArrayDeserializer(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public DeserializationResult<byte[]> deserializeChunk(InputStream input) throws IOException {
        List<byte[]> result = new ArrayList<>();
        byte[] buffer = new byte[chunkSize];
        int bytesReadTotal = 0;

        int bytesRead;
        while ((bytesRead = input.read(buffer)) != -1) {
            if (bytesRead < chunkSize) {
                // Partial chunk, rewind and keep it for later
                break;
            }
            result.add(Arrays.copyOf(buffer, bytesRead));
            bytesReadTotal += bytesRead;
        }

        return new DeserializationResult<>(result, bytesReadTotal);
    }
}