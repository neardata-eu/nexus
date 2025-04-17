package io.nexus.streamlets.state;

import io.nexus.configuration.NexusConfig;
import io.nexus.streamlets.state.backends.InMemoryStateBackend;
import io.nexus.streamlets.state.backends.RedisStateBackend;
import io.nexus.streamlets.state.backends.RocksDBStateBackend;

public class StreamletStateBackendFactory {

    public static StreamletStateBackend createBackend(NexusConfig config) {
        return switch (config.getStateBackendType().toLowerCase()) {
            case "redis" -> new RedisStateBackend(config.getRedisStateBackendHost(), config.getRedisStateBackendPort());
            case "inmemory" -> new InMemoryStateBackend();
            case "rocksdb" -> new RocksDBStateBackend(config.getRocksDBStateBackendPath());
            default -> throw new IllegalArgumentException("Unknown backend type: " + config.getStateBackendType());
        };
    }
}
