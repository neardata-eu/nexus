package io.nexus.streamlets.state.backends;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.streamlets.state.StreamletStateBackend;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;

/**
 * Implementation of {@link StreamletStateBackend} for RocksDB.
 */
public class RocksDBStateBackend implements StreamletStateBackend {
    private final RocksDB rocksDB;
    private final ObjectMapper objectMapper;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBStateBackend(String dbPath) {
        try {
            Options options = new Options().setCreateIfMissing(true);
            this.rocksDB = RocksDB.open(options, dbPath);
            this.objectMapper = new ObjectMapper();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
    }

    @Override
    public <T> void save(String key, T value) {
        try {
            this.rocksDB.put(key.getBytes(), objectMapper.writeValueAsBytes(value));
        } catch (Exception e) {
            throw new RuntimeException("Failed to save value to RocksDB", e);
        }
    }

    @Override
    public <T> T load(String key, Class<T> type) {
        try {
            byte[] data = this.rocksDB.get(key.getBytes());
            return data != null ? this.objectMapper.readValue(data, type) : null;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load value from RocksDB", e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            this.rocksDB.delete(key.getBytes());
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete value from RocksDB", e);
        }
    }

    public void close() {
        this.rocksDB.close();
    }
}

