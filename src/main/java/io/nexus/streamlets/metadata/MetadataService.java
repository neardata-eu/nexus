package io.nexus.streamlets.metadata;

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

    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataService(JedisPool jedis) {
        logger.info("Instantiating MetadataService");
        this.jedisPool = jedis;
    }

    // Create/Update Policy
    public void savePolicy(Policy policy) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            logger.info("Saving policy " + policy.toString());
            String key = METADATA_POLICY_PREFIX + policy.getId();
            String json = this.objectMapper.writeValueAsString(policy);
            jedis.set(key, json);
        } catch (Exception e) {
            logger.warn("Error while saving policy in metadata");
            throw new Exception(e);
        }
    }

    // Read Policy
    public Policy getPolicy(String key) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(key);
            return this.objectMapper.readValue(json, Policy.class);
        } catch (Exception e) {
            logger.warn("Error while getting policy from metadata service");
            throw new Exception(e);
        }
    }

    public Policy getPolicyByScope(String scope) throws Exception {
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
            throw new Exception(e);
        }
    }

    public Policy getPolicyByStream(String scope, String stream) throws Exception {
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
            throw new Exception(e);
        }
    }

    // Delete Policy
    public void deletePolicy(String id) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_POLICY_PREFIX + id;
            jedis.del(key);
        } catch (Exception e) {
            logger.warn("Error while deleting policy from metadata service");
            throw new Exception(e);
        }

    }

    // Similarly implement for StreamletDescriptor
    public void saveStreamletDescriptor(StreamletDescriptor descriptor) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_STREAMLET_PREFIX + descriptor.getId();
            String json = this.objectMapper.writeValueAsString(descriptor);
            jedis.set(key, json);
        } catch (Exception e) {
            logger.warn("Error while saving streamlet information into metadata service");
            throw new Exception(e);
        }
    }

    public StreamletDescriptor getStreamletDescriptor(String id) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_STREAMLET_PREFIX + id;
            String json = jedis.get(key);
            return this.objectMapper.readValue(json, StreamletDescriptor.class);
        } catch (Exception e) {
            logger.warn("Error while getting streamlet information from metadata service");
            throw new Exception(e);
        }
    }

    public void deleteStreamletDescriptor(String id) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_STREAMLET_PREFIX + id;
            jedis.del(key);
        } catch (Exception e) {
            logger.warn("Error while deleting streamlet information from metadata service");
            throw new Exception(e);
        }
    }

    // Similarly implement for SwarmletDescriptor
    public void saveSwarmletDescriptor(SwarmletDescriptor descriptor) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_SWARMLET_PREFIX + descriptor.getServiceEndpoint();
            String json = this.objectMapper.writeValueAsString(descriptor);
            jedis.set(key, json);
        } catch (Exception e) {
            logger.warn("Error while saving swarmlet information into metadata service");
            throw new Exception(e);
        }
    }

    public SwarmletDescriptor getSwarmletDescriptor(String id) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_SWARMLET_PREFIX + id;
            String json = jedis.get(key);
            return this.objectMapper.readValue(json, SwarmletDescriptor.class);
        } catch (Exception e) {
            logger.warn("Error while getting swarmlet information from metadata service");
            throw new Exception(e);
        }
    }

    public void deleteSwarmletDescriptor(String id) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = METADATA_SWARMLET_PREFIX + id;
            jedis.del(key);
        } catch (Exception e) {
            logger.warn("Error while deleting swarmlet information from metadata service");
            throw new Exception(e);
        }

    }
}
