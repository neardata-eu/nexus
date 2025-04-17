package io.nexus.streamlets.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MetadataChangeNotifier implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MetadataChangeNotifier.class);
    private final List<MetadataCallback> callbacks = new ArrayList<>();
    private final JedisPool jedisPool;
    private Thread subscriberThread;
    private volatile boolean running = true;

    public MetadataChangeNotifier(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void registerCallback(MetadataCallback callback) {
        callbacks.add(callback);
    }

    private void notifyCallbacks(String key, String message) {
        for (MetadataCallback callback : callbacks) {
            callback.onMessage(key, message);
        }
    }

    public void initializeSubscriber() {
        this.subscriberThread = new Thread(() -> {
            while (this.running) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.psubscribe(new JedisPubSub() {
                        @Override
                        public void onPMessage(String pattern, String channel, String message) {
                            logger.debug("Received Redis event: {} on channel: {}", message, channel);
                            // Extract the actual key from "__keyspace@0__:policy:policy-6"
                            String key = channel.replace("__keyspace@0__:", "");
                            // Invoke specific callbacks from other classes
                            notifyCallbacks(key, message);
                        }
                    // Subscribe to all metadata-related keyspace events
                    }, "__keyspace@0__:" + MetadataService.METADATA_MEMBERSHIP_PREFIX + "*",
                            "__keyspace@0__:" + MetadataService.METADATA_S3_PREFIX + "*",
                            "__keyspace@0__:" + MetadataService.METADATA_POLICY_PREFIX + "*",
                            "__keyspace@0__:" + MetadataService.METADATA_STREAMLET_PREFIX + "*",
                            "__keyspace@0__:" + MetadataService.METADATA_SWARMLET_PREFIX + "*",
                            "__keyspace@0__:" + MetadataService.METADATA_STREAMLET_CODE_PREFIX + "*");
                } catch (Exception e) {
                    logger.error("Redis subscription error, retrying in 5 seconds...", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ignored) {}
                }
            }
        });
        this.subscriberThread.start();
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (subscriberThread != null) {
            subscriberThread.interrupt();
            try {
                subscriberThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Failed to close RedisSubscriber", e);
            }
        }
    }
}
