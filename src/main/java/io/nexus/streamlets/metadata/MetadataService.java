package io.nexus.streamlets.metadata;

import io.nexus.configuration.NexusConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;

public class MetadataService {
    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    public static final String METADATA_POLICY_PREFIX = "policy:";
    public static final String METADATA_STREAMLET_PREFIX = "streamletdescriptor:";
    public static final String METADATA_SWARMLET_PREFIX = "swarmletdescriptor:";

    private final NexusConfig nexusConfig;
    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataService(NexusConfig nexusConfig, JedisPool jedis) {
        logger.info("Instantiating MetadataService");
        this.nexusConfig = nexusConfig;
        this.jedisPool = jedis;
    }

    // Read Policy

    public Policy getPolicy(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(key);
            return this.objectMapper.readValue(json, Policy.class);
        } catch (Exception e) {
            logger.warn("Error while getting policy from metadata service");
            throw new RuntimeException(e);
        }
    }

    public Policy getPolicyByScope(String scope) {
        // TODO: This can be done more efficiently.
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.keys(METADATA_POLICY_PREFIX + "*");
            for (String key : keys) {
                Policy policy = getPolicy(key);
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

    public Policy getPolicyByStream(String scope, String stream) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.keys(METADATA_POLICY_PREFIX + "*");
            for (String key : keys) {
                Policy policy = getPolicy(key);
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

    public StreamletDescriptor getStreamletDescriptor(String id) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_STREAMLET_PREFIX + id;
            String json = jedis.get(key);
            return this.objectMapper.readValue(json, StreamletDescriptor.class);
        } catch (Exception e) {
            logger.warn("Error while getting streamlet information from metadata service");
            throw new RuntimeException(e);
        }
    }

    public SwarmletDescriptor getSwarmletDescriptor(String id) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_SWARMLET_PREFIX + id;
            String json = jedis.get(key);
            return this.objectMapper.readValue(json, SwarmletDescriptor.class);
        } catch (Exception e) {
            logger.warn("Error while getting swarmlet information from metadata service");
            throw new RuntimeException(e);
        }
    }

    public NexusConfig getNexusConfig() {
        return nexusConfig;
    }

    public String getSwarmletDescriptorByRegionAndHardware(Region currentRegion, Hardware requiredHardware) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.keys(METADATA_SWARMLET_PREFIX + "*");
            for (String key : keys) {
                SwarmletDescriptor swarmletDescriptor = getSwarmletDescriptor(key.replace(METADATA_SWARMLET_PREFIX, ""));
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
