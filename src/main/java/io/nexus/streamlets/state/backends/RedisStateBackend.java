package io.nexus.streamlets.state.backends;

import io.nexus.streamlets.state.StreamletStateBackend;
import io.nexus.streamlets.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;

/**
 * Implementation of {@link StreamletStateBackend} for Redis.
 */
public class RedisStateBackend implements StreamletStateBackend {
    final Logger logger = LoggerFactory.getLogger(RedisStateBackend.class);
    private final Jedis jedis;
    private static final int MAX_RETRIES = 3;

    public RedisStateBackend(String host, int port) {
        this.jedis = new Jedis(host, port);
    }

    @Override
    public <T> void save(String key, T value) {
        byte[] serializedValue = SerializationUtils.kryoSerialize(value);
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                jedis.watch(key.getBytes());
                Transaction transaction = jedis.multi();
                transaction.set(key.getBytes(), serializedValue);
                List<Object> result = transaction.exec();
                if (result != null) {
                    return; // Transaction successful
                }
            } catch (JedisException e) {
                logger.error("Error while attempting to save data in Redis, retrying {}", key);
            } finally {
                jedis.unwatch();
            }
            retries++;
        }
        throw new JedisException("Failed to save key after " + MAX_RETRIES + " retries.");
    }

    @Override
    public <T> T load(String key, Class<T> type) {
        byte[] data = jedis.get(key.getBytes());
        return data != null ? SerializationUtils.kryoDeserialize(data, type) : null;
    }

    @Override
    public void delete(String key) {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                jedis.watch(key.getBytes());
                Transaction transaction = jedis.multi();
                transaction.del(key.getBytes());
                List<Object> result = transaction.exec();
                if (result != null) {
                    return; // Transaction successful
                }
            } catch (JedisException e) {
                logger.error("Error while attempting to delete data in Redis, retrying {}", key);
            } finally {
                jedis.unwatch();
            }
            retries++;
        }
        throw new JedisException("Failed to delete key after " + MAX_RETRIES + " retries.");
    }
}