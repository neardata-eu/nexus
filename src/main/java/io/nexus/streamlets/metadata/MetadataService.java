package io.nexus.streamlets.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;

public class MetadataService {
    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    public static final String METADATA_POLICY_PREFIX = "policy:";
    public static final String METADATA_STREAMLET_PREFIX = "streamletdescriptor:";
    public static final String METADATA_SWARMLET_PREFIX = "swarmletdescriptor:";

    private final Jedis jedis;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataService(Jedis jedis) {
        logger.info("Instantiating MetadataService");
        this.jedis = jedis;
    }

    // Create/Update Policy
    public void savePolicy(Policy policy) throws Exception {
        logger.info("Saving policy " + policy.toString());
        String key = METADATA_POLICY_PREFIX + policy.getId();
        String json = this.objectMapper.writeValueAsString(policy);
        this.jedis.set(key, json);
    }

    // Read Policy
    public Policy getPolicy(String key) throws Exception {
        String json = this.jedis.get(key);
        return this.objectMapper.readValue(json, Policy.class);
    }

    public Policy getPolicyByScope(String scope) throws Exception {
        // TODO: This can be done more efficiently.
        Set<String> keys = this.jedis.keys(METADATA_POLICY_PREFIX + "*");
        for (String key : keys) {
            Policy policy = getPolicy(key);
            if (policy.getScope().equals(scope)) {
                return policy;
            }
        }
        return null;
    }

    public Policy getPolicyByStream(String scope, String stream) throws Exception {
        Set<String> keys = this.jedis.keys(METADATA_POLICY_PREFIX + "*");
        for (String key : keys) {
            Policy policy = getPolicy(key);
            if (policy.getScope().equals(scope) && policy.getStream().equals(stream)) {
                return policy;
            }
        }
        return null;
    }

    // Delete Policy
    public void deletePolicy(String id) {
        String key = METADATA_POLICY_PREFIX + id;
        this.jedis.del(key);
    }

    // Similarly implement for StreamletDescriptor
    public void saveStreamletDescriptor(StreamletDescriptor descriptor) throws Exception {
        String key = METADATA_STREAMLET_PREFIX + descriptor.getId();
        String json = this.objectMapper.writeValueAsString(descriptor);
        this.jedis.set(key, json);
    }

    public StreamletDescriptor getStreamletDescriptor(String id) throws Exception {
        String key = METADATA_STREAMLET_PREFIX + id;
        String json = this.jedis.get(key);
        return this.objectMapper.readValue(json, StreamletDescriptor.class);
    }

    public void deleteStreamletDescriptor(String id) {
        String key = METADATA_STREAMLET_PREFIX + id;
        this.jedis.del(key);
    }

    // Similarly implement for SwarmletDescriptor
    public void saveSwarmletDescriptor(SwarmletDescriptor descriptor) throws Exception {
        String key = METADATA_SWARMLET_PREFIX + descriptor.getId();
        String json = this.objectMapper.writeValueAsString(descriptor);
        this.jedis.set(key, json);
    }

    public SwarmletDescriptor getSwarmletDescriptor(String id) throws Exception {
        String key = METADATA_SWARMLET_PREFIX + id;
        String json = this.jedis.get(key);
        return this.objectMapper.readValue(json, SwarmletDescriptor.class);
    }

    public void deleteSwarmletDescriptor(String id) {
        String key = METADATA_SWARMLET_PREFIX + id;
        this.jedis.del(key);
    }
}
