package io.nexus.streamlets.state.backends;

import io.nexus.streamlets.state.StreamletStateBackend;
import io.nexus.streamlets.utils.SerializationUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of StateBackend using a hashmap for testing and
 * development purposes.
 */
public class InMemoryStateBackend implements StreamletStateBackend {

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public <T> void save(String key, T value) {
        try {
            this.storage.put(key, SerializationUtils.kryoSerialize(value));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize and save value", e);
        }
    }

    @Override
    public <T> T load(String key, Class<T> type) {
        try {
            byte[] data = this.storage.get(key);
            return data != null ? SerializationUtils.kryoDeserialize(data, type) : null;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize and load value", e);
        }
    }

    @Override
    public void delete(String key) {
        this.storage.remove(key);
    }
}
