package io.nexus.streamlets.metadata;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MetadataServiceTest {

    // TODO: See what's wrong with this test
    // @Test
    public void testGetPolicy() throws Exception {
        // Given
        JedisPool mockJedisPool = Mockito.mock(JedisPool.class);
        Jedis mockJedis = Mockito.mock(Jedis.class);
        when(mockJedisPool.getResource()).thenReturn(mockJedis);

        MetadataService metadataService = new MetadataService(mockJedisPool);
        ObjectMapper objectMapper = new ObjectMapper();

        Policy expectedPolicy = new Policy("policy123", "kafka", "myScope", "myStream", List.of("edge(s1) | cloud(s2)"),
                List.of("bucket1", "local_store"));
        String policyJson = objectMapper.writeValueAsString(expectedPolicy);

        // Mock the Redis response
        when(mockJedisPool.getResource().get("policy:policy123")).thenReturn(policyJson);

        // When
        Policy actualPolicy = metadataService.getPolicy("policy123");

        // Then
        verify(mockJedisPool, times(1)).getResource().get("policy:policy123"); // Ensure Redis GET is called once
        assertNotNull(actualPolicy); // Policy object should not be null
        assertEquals(expectedPolicy.getId(), actualPolicy.getId()); // Validate ID
        assertEquals(expectedPolicy.getSystem(), actualPolicy.getSystem()); // Validate System
        assertEquals(expectedPolicy.getScope(), actualPolicy.getScope()); // Validate Scope
        assertEquals(expectedPolicy.getStream(), actualPolicy.getStream()); // Validate Scope
        assertEquals(expectedPolicy.getPipeline(), actualPolicy.getPipeline()); // Validate Pipeline
        assertEquals(expectedPolicy.getStorage(), actualPolicy.getStorage()); // Validate Storage
    }
}
