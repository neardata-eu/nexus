package io.nexus.streamlets.metadata;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

public class MetadataServiceTest {

    // TODO: See what's wrong with this test
    // @Test
    public void testGetPolicy() throws Exception {
        // Given
        Jedis mockJedis = Mockito.mock(Jedis.class);
        MetadataService metadataService = new MetadataService(mockJedis);
        ObjectMapper objectMapper = new ObjectMapper();

        Policy expectedPolicy = new Policy("policy123", "kafka", "myScope", "myStream",
                List.of("edge(s1) | cloud(s2)"), List.of("bucket1", "local_store"));
        String policyJson = objectMapper.writeValueAsString(expectedPolicy);

        // Mock the Redis response
        when(mockJedis.get("policy:policy123")).thenReturn(policyJson);

        // When
        Policy actualPolicy = metadataService.getPolicy("policy123");

        // Then
        verify(mockJedis, times(1)).get("policy:policy123"); // Ensure Redis GET is called once
        assertNotNull(actualPolicy); // Policy object should not be null
        assertEquals(expectedPolicy.getId(), actualPolicy.getId()); // Validate ID
        assertEquals(expectedPolicy.getSystem(), actualPolicy.getSystem()); // Validate System
        assertEquals(expectedPolicy.getScope(), actualPolicy.getScope()); // Validate Scope
        assertEquals(expectedPolicy.getStream(), actualPolicy.getStream()); // Validate Scope
        assertEquals(expectedPolicy.getPipeline(), actualPolicy.getPipeline()); // Validate Pipeline
        assertEquals(expectedPolicy.getStorage(), actualPolicy.getStorage()); // Validate Storage
    }
}
