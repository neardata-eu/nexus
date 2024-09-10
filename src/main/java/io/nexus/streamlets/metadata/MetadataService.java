package io.nexus.streamlets.metadata;

import io.nexus.streamlets.StreamletsExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class MetadataService {
    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    private static final String METADATA_POLICY_PREFIX = "policy:";
    private static final String METADATA_STREAMLET_PREFIX = "streamletdescriptor:";
    private static final String METADATA_SWARMLET_PREFIX = "swarmletdescriptor:";

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
    public Policy getPolicy(String id) throws Exception {
        String key = METADATA_POLICY_PREFIX + id;
        String json = this.jedis.get(key);
        return this.objectMapper.readValue(json, Policy.class);
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
