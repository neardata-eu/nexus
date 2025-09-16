package io.nexus.streamlets.state.backends;

import io.nexus.streamlets.state.StreamletStateBackend;
import io.nexus.streamlets.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;

/**
 * Implementation of {@link StreamletStateBackend} for Redis.
 */
public class RedisStateBackend implements StreamletStateBackend {
    final Logger logger = LoggerFactory.getLogger(RedisStateBackend.class);
    private final JedisPool jedisPool;
    private static final int MAX_RETRIES = 3;
    private static final int CONNECTION_TIMEOUT = 2000;

    public RedisStateBackend(String host, int port) {
        // Create connection pool configuration
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestOnCreate(true);
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(2);

        // Use the correct constructor - this works for most Jedis versions
        this.jedisPool = new JedisPool(poolConfig, host, port, CONNECTION_TIMEOUT);
    }

    @Override
    public <T> void save(String key, T value) {
        byte[] serializedValue = SerializationUtils.kryoSerialize(value);
        int retries = 0;

        while (retries < MAX_RETRIES) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                // Reset any previous state
                jedis.resetState();

                // Check connection health
                if (!isConnectionHealthy(jedis)) {
                    throw new JedisConnectionException("Connection failed health check");
                }

                jedis.watch(key.getBytes());
                Transaction transaction = jedis.multi();
                transaction.set(key.getBytes(), serializedValue);
                List<Object> result = transaction.exec();

                if (result != null) {
                    return; // Transaction successful
                }
                // If result is null, it means the watched key was modified
                // Continue to retry

            } catch (JedisException e) {
                logger.error("Error while attempting to save data in Redis, retrying {} (attempt {}/{})",
                        key, retries + 1, MAX_RETRIES, e);

                // On connection errors, discard the connection
                if (jedis != null && isConnectionError(e)) {
                    try {
                        jedis.close(); // This marks the connection as broken
                    } catch (Exception closeEx) {
                        logger.warn("Error closing broken connection", closeEx);
                    }
                    jedis = null; // Prevent cleanup in finally block
                }

            } finally {
                if (jedis != null) {
                    try {
                        jedis.resetState(); // Clear any MULTI/WATCH state
                        jedis.close(); // Return to pool
                    } catch (Exception e) {
                        logger.warn("Error cleaning up Jedis connection", e);
                    }
                }
            }

            retries++;

            // Brief delay before retry to avoid hammering Redis
            if (retries < MAX_RETRIES) {
                try {
                    Thread.sleep(50L * retries);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new JedisException("Interrupted during retry", ie);
                }
            }
        }

        throw new JedisException("Failed to save key '" + key + "' after " + MAX_RETRIES + " retries.");
    }

    @Override
    public <T> T load(String key, Class<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Reset state (shouldn't be necessary for simple GET, but good practice)
            jedis.resetState();

            byte[] data = jedis.get(key.getBytes());
            return data != null ? SerializationUtils.kryoDeserialize(data, type) : null;

        } catch (JedisException e) {
            logger.error("Error while attempting to load data from Redis for key {}", key, e);
            throw new JedisException("Failed to load key '" + key + "'", e);
        }
    }

    @Override
    public void delete(String key) {
        int retries = 0;

        while (retries < MAX_RETRIES) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                // Reset any previous state
                jedis.resetState();

                // Check connection health
                if (!isConnectionHealthy(jedis)) {
                    throw new JedisConnectionException("Connection failed health check");
                }

                jedis.watch(key.getBytes());
                Transaction transaction = jedis.multi();
                transaction.del(key.getBytes());
                List<Object> result = transaction.exec();

                if (result != null) {
                    return; // Transaction successful
                }
                // If result is null, it means the watched key was modified
                // Continue to retry

            } catch (JedisException e) {
                logger.error("Error while attempting to delete data in Redis, retrying {} (attempt {}/{})",
                        key, retries + 1, MAX_RETRIES, e);

                // On connection errors, discard the connection
                if (jedis != null && isConnectionError(e)) {
                    try {
                        jedis.close(); // This marks the connection as broken
                    } catch (Exception closeEx) {
                        logger.warn("Error closing broken connection", closeEx);
                    }
                    jedis = null; // Prevent cleanup in finally block
                }

            } finally {
                if (jedis != null) {
                    try {
                        jedis.resetState(); // Clear any MULTI/WATCH state
                        jedis.close(); // Return to pool
                    } catch (Exception e) {
                        logger.warn("Error cleaning up Jedis connection", e);
                    }
                }
            }

            retries++;

            // Brief delay before retry
            if (retries < MAX_RETRIES) {
                try {
                    Thread.sleep(50 * retries);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new JedisException("Interrupted during retry", ie);
                }
            }
        }

        throw new JedisException("Failed to delete key '" + key + "' after " + MAX_RETRIES + " retries.");
    }

    /**
     * Check if the Jedis connection is healthy
     */
    private boolean isConnectionHealthy(Jedis jedis) {
        try {
            return "PONG".equals(jedis.ping());
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Check if the exception is a connection-related error
     */
    private boolean isConnectionError(Exception e) {
        return e instanceof JedisConnectionException ||
                (e.getCause() instanceof JedisConnectionException) ||
                e.getMessage().contains("connection") ||
                e.getMessage().contains("broken");
    }

    /**
     * Clean shutdown of the connection pool
     */
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }
}