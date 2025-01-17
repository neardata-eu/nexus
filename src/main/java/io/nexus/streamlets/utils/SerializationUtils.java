package io.nexus.streamlets.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

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
}

