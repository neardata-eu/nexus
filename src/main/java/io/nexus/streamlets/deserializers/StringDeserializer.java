package io.nexus.streamlets.deserializers;

import io.nexus.streamlets.DeserializationResult;
import io.nexus.streamlets.Deserializer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StringDeserializer implements Deserializer<String> {

    @Override
    public DeserializationResult<String> deserializeChunk(InputStream input) throws IOException {
        List<String> result = new ArrayList<>();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int ch;
        int bytesConsumed = 0;

        while ((ch = input.read()) != -1) {
            buffer.write(ch);
            bytesConsumed++;

            if (ch == '\n') {
                String line = buffer.toString(StandardCharsets.UTF_8);
                result.add(line.stripTrailing()); // strip newline
                buffer.reset();
            }
        }

        // If we have partial line data left, rewind the bytesConsumed
        bytesConsumed -= buffer.size(); // these bytes belong to an incomplete record

        return new DeserializationResult<>(result, bytesConsumed);
    }
}