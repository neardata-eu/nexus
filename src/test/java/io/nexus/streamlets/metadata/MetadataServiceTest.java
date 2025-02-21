package io.nexus.streamlets.metadata;

import io.nexus.configuration.NexusConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MetadataServiceTest {

    private NexusConfig nexusConfig;
    private JedisPool jedisPool;
    private Jedis jedis;
    private MetadataService metadataService;

    @BeforeEach
    public void setUp() {
        nexusConfig = mock(NexusConfig.class);
        jedisPool = mock(JedisPool.class);
        jedis = mock(Jedis.class);

        when(jedisPool.getResource()).thenReturn(jedis);

        metadataService = new MetadataService(nexusConfig, jedisPool);
    }

    @Test
    public void testGetPolicy() throws Exception {
        String key = "policy:test";
        String json = "{\"scope\":\"testScope\",\"stream\":\"testStream\"}";
        Policy expectedPolicy = new ObjectMapper().readValue(json, Policy.class);

        metadataService.policyCache.put(key, expectedPolicy);

        Policy policy = metadataService.getPolicy(key);
        assertNotNull(policy);
        assertEquals("testScope", policy.getScope());
        assertEquals("testStream", policy.getStream());
    }

    @Test
    public void testGetPolicyByScope() throws Exception {
        String key = "policy:test";
        String json = "{\"scope\":\"testScope\",\"stream\":\"testStream\"}";
        Policy expectedPolicy = new ObjectMapper().readValue(json, Policy.class);

        metadataService.policyCache.put(key, expectedPolicy);

        Policy policy = metadataService.getPolicyByScope("testScope");
        assertNotNull(policy);
        assertEquals("testScope", policy.getScope());
    }

    @Test
    public void testGetPolicyByStream() throws Exception {
        String key = "policy:test";
        String json = "{\"scope\":\"testScope\",\"stream\":\"testStream\"}";
        Policy expectedPolicy = new ObjectMapper().readValue(json, Policy.class);

        metadataService.policyCache.put(key, expectedPolicy);

        Policy policy = metadataService.getPolicyByStream("testScope", "testStream");
        assertNotNull(policy);
        assertEquals("testStream", policy.getStream());
    }

    @Test
    public void testGetStreamletDescriptor() throws Exception {
        String id = "test";
        String key = "streamletdescriptor:" + id;
        StreamletDescriptor expectedDescriptor = new StreamletDescriptor(id, StreamletDescriptor.ExecuteOn.ALL,
                Hardware.GPU, true, true, false);

        metadataService.streamletCache.put(key, expectedDescriptor);

        StreamletDescriptor descriptor = metadataService.getStreamletDescriptor(id);
        assertNotNull(descriptor);
        assertEquals("test", descriptor.getId());
        assertEquals(StreamletDescriptor.ExecuteOn.ALL, descriptor.getExecuteOn());
        assertEquals(Hardware.GPU, descriptor.getHardware());
        assertTrue(descriptor.isPartitionLocality());
    }

    @Test
    public void testGetSwarmletDescriptor() throws Exception {
        String id = "test";
        String key = "swarmletdescriptor:" + id;
        SwarmletDescriptor expectedDescriptor = new SwarmletDescriptor("testEndpoint",
                Region.EDGE, Hardware.GPU);

        metadataService.swarmletCache.put(key, expectedDescriptor);

        SwarmletDescriptor descriptor = metadataService.getSwarmletDescriptor(id);
        assertNotNull(descriptor);
        assertEquals("testEndpoint", descriptor.getServiceEndpoint());
        assertEquals(Region.EDGE, descriptor.getRegion());
        assertEquals(Hardware.GPU, descriptor.getHardware());
    }

    @Test
    public void testGetSwarmletDescriptorByRegionAndHardware() throws Exception {
        String key = "swarmletdescriptor:test";
        SwarmletDescriptor expectedDescriptor = new SwarmletDescriptor("testEndpoint",
                Region.CLOUD, Hardware.NONE);

        metadataService.swarmletCache.put(key, expectedDescriptor);

        String endpoint = metadataService.getSwarmletDescriptorByRegionAndHardware(Region.CLOUD, Hardware.NONE);
        assertNotNull(endpoint);
        assertEquals("testEndpoint", endpoint);
    }
}
