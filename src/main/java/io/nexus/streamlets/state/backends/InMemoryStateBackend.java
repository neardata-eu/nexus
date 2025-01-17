package io.nexus.streamlets.state.backends;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.streamlets.state.StreamletStateBackend;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of StateBackend using a hashmap for testing and
 * development purposes.
 */
public class InMemoryStateBackend implements StreamletStateBackend {

    private final Map<String, String> storage = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public <T> void save(String key, T value) {
        try {
            this.storage.put(key, this.objectMapper.writeValueAsString(value));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize and save value", e);
        }
    }

    @Override
    public <T> T load(String key, Class<T> type) {
        try {
            String data = this.storage.get(key);
            return data != null ? this.objectMapper.readValue(data, type) : null;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize and load value", e);
        }
    }

    @Override
    public void delete(String key) {
        this.storage.remove(key);
    }
}
