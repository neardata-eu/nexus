package io.nexus.streamlets;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for deserializing records from an input stream.
 *
 * @param <T> The type of record.
 */
public interface Deserializer<T> {
    /**
     * Deserializes a record from the given input stream.
     *
     * @param input The input stream.
     * @return The deserialized record.
     * @throws java.io.IOException If deserialization fails.
     */
    T deserialize(InputStream input) throws IOException;
}
