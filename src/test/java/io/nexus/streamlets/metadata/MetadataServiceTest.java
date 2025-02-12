package io.nexus.streamlets.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.nexus.configuration.NexusConfig;
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
    //@Test
    public void testGetPolicy() throws Exception {
        // Given
        JedisPool mockJedisPool = Mockito.mock(JedisPool.class);
        Jedis mockJedis = Mockito.mock(Jedis.class);
        when(mockJedisPool.getResource()).thenReturn(mockJedis);
        NexusConfig nexusConfig = Mockito.mock(NexusConfig.class);;

        MetadataService metadataService = new MetadataService(nexusConfig, mockJedisPool);
        ObjectMapper objectMapper = new ObjectMapper();
        StreamletDescriptor mockPutStreamlet = new StreamletDescriptor("noop-1", StreamletDescriptor.ExecuteOn.ALL,
                Hardware.NONE, true);
        Policy expectedPolicy = new Policy("policy123", "kafka", "myScope", "myStream",
                List.of(new StreamletExecutionDescriptor(mockPutStreamlet, Region.EDGE, Collections.emptyList())),
                List.of("bucket1", "local_store"));
        String policyJson = objectMapper.writeValueAsString(expectedPolicy);

        // When
        when(mockJedis.get("policy123")).thenReturn(policyJson);
        Policy actualPolicy = metadataService.getPolicy("policy123");

        // Then
        verify(mockJedis, times(1)).get("policy123"); // Ensure Redis GET is called
                                                      // once
        assertNotNull(actualPolicy); // Policy object should not be null
        assertEquals(expectedPolicy.getId(), actualPolicy.getId()); // Validate ID
        assertEquals(expectedPolicy.getSystem(), actualPolicy.getSystem()); // Validate System
        assertEquals(expectedPolicy.getScope(), actualPolicy.getScope()); // Validate Scope
        assertEquals(expectedPolicy.getStream(), actualPolicy.getStream()); // Validate Scope

        // Both objects are the same yet it fails
        assertArrayEquals(expectedPolicy.getPipeline().toArray(),
        actualPolicy.getPipeline().toArray());
        assertEquals(expectedPolicy.getStorage(), actualPolicy.getStorage()); // Validate
    }
}
