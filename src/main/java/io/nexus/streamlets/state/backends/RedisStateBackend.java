package io.nexus.streamlets.state.backends;

import io.nexus.streamlets.state.StreamletStateBackend;
import io.nexus.streamlets.utils.SerializationUtils;
import redis.clients.jedis.Jedis;

/**
 * Implementation of {@link StreamletStateBackend} for Redis.
 */
public class RedisStateBackend implements StreamletStateBackend {
    private final Jedis jedis;

    public RedisStateBackend(String host, int port) {
        this.jedis = new Jedis(host, port);
    }

    @Override
    public <T> void save(String key, T value) {
        jedis.set(key, SerializationUtils.serialize(value));
    }

    @Override
    public <T> T load(String key, Class<T> type) {
        String data = jedis.get(key);
        return data != null ? SerializationUtils.deserialize(data, type) : null;
    }

    @Override
    public void delete(String key) {
        jedis.del(key);
    }
}

