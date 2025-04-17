package io.nexus.streamlets;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface Deserializer<T> {
    /**
     * Deserializes as many records as possible from the input stream.
     *
     * @param input The input stream.
     * @return List of deserialized records.
     * @throws IOException If deserialization fails.
     */
    DeserializationResult<T> deserializeChunk(InputStream input) throws IOException;
}
