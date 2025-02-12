package io.nexus.streamlets.metadata;

import io.nexus.configuration.NexusConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataService {
    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    public static final String METADATA_POLICY_PREFIX = "policy:";
    public static final String METADATA_STREAMLET_PREFIX = "streamletdescriptor:";
    public static final String METADATA_SWARMLET_PREFIX = "swarmletdescriptor:";

    private final NexusConfig nexusConfig;
    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper = new ObjectMapper();
    final Map<String, Policy> policyCache = new ConcurrentHashMap<>();
    final Map<String, StreamletDescriptor> streamletCache = new ConcurrentHashMap<>();
    final Map<String, SwarmletDescriptor> swarmletCache = new ConcurrentHashMap<>();

    /**
     * Constructs a MetadataService instance.
     *
     * @param nexusConfig the Nexus configuration
     * @param jedisPool   the Jedis connection pool
     */
    public MetadataService(NexusConfig nexusConfig, JedisPool jedisPool) {
        logger.info("Instantiating MetadataService");
        this.nexusConfig = nexusConfig;
        this.jedisPool = jedisPool;
        loadInitialData();
        initializeSubscriber();
    }

    /**
     * Loads initial data from Redis into local caches.
     */
    private void loadInitialData() {
        try (Jedis jedis = jedisPool.getResource()) {
            loadPolicies(jedis);
            loadStreamletDescriptors(jedis);
            loadSwarmletDescriptors(jedis);
        } catch (Exception e) {
            logger.error("Error while loading initial data from Redis", e);
        }
    }

    private void loadPolicies(Jedis jedis) {
        Set<String> keys = jedis.keys(METADATA_POLICY_PREFIX + "*");
        for (String key : keys) {
            try {
                String json = jedis.get(key);
                Policy policy = objectMapper.readValue(json, Policy.class);
                policyCache.put(key, policy);
            } catch (Exception e) {
                logger.error("Error while loading policy with key: " + key, e);
            }
        }
    }

    private void loadStreamletDescriptors(Jedis jedis) {
        Set<String> keys = jedis.keys(METADATA_STREAMLET_PREFIX + "*");
        for (String key : keys) {
            try {
                String json = jedis.get(key);
                StreamletDescriptor descriptor = objectMapper.readValue(json, StreamletDescriptor.class);
                streamletCache.put(key, descriptor);
            } catch (Exception e) {
                logger.error("Error while loading streamlet descriptor with key: " + key, e);
            }
        }
    }

    private void loadSwarmletDescriptors(Jedis jedis) {
        Set<String> keys = jedis.keys(METADATA_SWARMLET_PREFIX + "*");
        for (String key : keys) {
            try {
                String json = jedis.get(key);
                SwarmletDescriptor descriptor = objectMapper.readValue(json, SwarmletDescriptor.class);
                swarmletCache.put(key, descriptor);
            } catch (Exception e) {
                logger.error("Error while loading swarmlet descriptor with key: " + key, e);
            }
        }
    }

    /**
     * Initializes the Redis subscriber to listen for updates and update local caches.
     */
    private void initializeSubscriber() {
        new Thread(() -> {
            while (true) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.psubscribe(new JedisPubSub() {
                        @Override
                        public void onPMessage(String pattern, String channel, String message) {
                            logger.info("Received Redis event: {} on channel: {}", message, channel);

                            // Extract the actual key from "__keyspace@0__:policy:policy-6"
                            String key = channel.replace("__keyspace@0__:*", "");

                            // Only handle "set" events
                            if ("set".equals(message)) {
                                try (Jedis innerJedis = jedisPool.getResource()) {
                                    String json = innerJedis.get(key);
                                    if (key.startsWith(METADATA_POLICY_PREFIX)) {
                                        policyCache.put(key, objectMapper.readValue(json, Policy.class));
                                    } else if (key.startsWith(METADATA_STREAMLET_PREFIX)) {
                                        streamletCache.put(key, objectMapper.readValue(json, StreamletDescriptor.class));
                                    } else if (key.startsWith(METADATA_SWARMLET_PREFIX)) {
                                        swarmletCache.put(key, objectMapper.readValue(json, SwarmletDescriptor.class));
                                    }
                                } catch (Exception e) {
                                    logger.error("Error updating cache for key: " + key, e);
                                }
                            }
                        }
                    }, "__keyspace@0__:*"); // Subscribe to all keyspace events
                } catch (Exception e) {
                    logger.error("Redis subscription error, retrying in 5 seconds...", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ignored) {}
                }
            }
        }).start();
    }

    // Read Policy

    /**
     * Retrieves a policy by its key.
     *
     * @param key the key of the policy
     * @return the Policy object
     */
    public Policy getPolicy(String key) {
        try {
            return this.policyCache.get(key);
        } catch (Exception e) {
            logger.warn("Error while getting policy from metadata service");
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves a policy by its scope.
     *
     * @param scope the scope of the policy
     * @return the Policy object
     */
    public Policy getPolicyByScope(String scope) {
        try {
            for (Policy policy : this.policyCache.values()) {
                if (policy.getScope().equals(scope)) {
                    return policy;
                }
            }
            return null;
        } catch (Exception e) {
            logger.warn("Error while getting policy by scope from metadata service");
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves a policy by its scope and stream.
     *
     * @param scope  the scope of the policy
     * @param stream the stream of the policy
     * @return the Policy object
     */
    public Policy getPolicyByStream(String scope, String stream) {
        try {
            for (Policy policy : this.policyCache.values()) {
                if (policy.getScope().equals(scope) && policy.getStream().equals(stream)) {
                    return policy;
                }
            }
            return null;
        } catch (Exception e) {
            logger.warn("Error while getting policy by stream from metadata service");
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves a streamlet descriptor by its ID.
     *
     * @param id the ID of the streamlet descriptor
     * @return the StreamletDescriptor object
     */
    public StreamletDescriptor getStreamletDescriptor(String id) {
        try {
            String key = METADATA_STREAMLET_PREFIX + id;
            return this.streamletCache.get(key);
        } catch (Exception e) {
            logger.warn("Error while getting streamlet information from metadata service");
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves a swarmlet descriptor by its ID.
     *
     * @param id the ID of the swarmlet descriptor
     * @return the SwarmletDescriptor object
     */
    public SwarmletDescriptor getSwarmletDescriptor(String id) {
        try {
            String key = METADATA_SWARMLET_PREFIX + id;
            return this.swarmletCache.get(key);
        } catch (Exception e) {
            logger.warn("Error while getting swarmlet information from metadata service");
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the Nexus configuration.
     *
     * @return the NexusConfig object
     */
    public NexusConfig getNexusConfig() {
        return this.nexusConfig;
    }

    /**
     * Retrieves a swarmlet descriptor by region and hardware requirements.
     *
     * @param currentRegion    the current region
     * @param requiredHardware the required hardware
     * @return the service endpoint of the swarmlet descriptor
     */
    public String getSwarmletDescriptorByRegionAndHardware(Region currentRegion, Hardware requiredHardware) {
        try {
            for (SwarmletDescriptor swarmletDescriptor : this.swarmletCache.values()) {
                if (swarmletDescriptor.getRegion().equals(currentRegion)
                        && (requiredHardware.equals(Hardware.NONE) || swarmletDescriptor.getHardware().equals(requiredHardware))) {
                    return swarmletDescriptor.getServiceEndpoint();
                }
            }
            return null;
        } catch (Exception e) {
            logger.warn("Error while getting Swarmlet by region/hardware from metadata service");
            throw new RuntimeException(e);
        }
    }
}