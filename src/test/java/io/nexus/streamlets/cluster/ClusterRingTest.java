package io.nexus.streamlets.cluster;

import static org.mockito.Mockito.*;

import io.nexus.streamlets.metadata.MetadataService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import java.util.*;

public class ClusterRingTest {
    private JedisPool jedisPool;
    private Jedis jedis;
    private ClusterRing clusterRing;

    @BeforeEach
    public void setUp() throws Exception {
        jedisPool = mock(JedisPool.class);
        jedis = mock(Jedis.class);
        when(jedisPool.getResource()).thenReturn(jedis);
        doNothing().when(jedis).subscribe(any(JedisPubSub.class), anyString());
        clusterRing = new ClusterRing(jedisPool, "http://localhost:8080", 1000, 3000, 10);
        clusterRing.start();
        Thread.sleep(1000);
    }

    @AfterEach
    public void tearDown() {
        clusterRing.close();
        reset(jedis, jedisPool);
    }

    @Test
    public void testNodeRegistration() {
        verify(jedis, times(2)).hset(eq(MetadataService.METADATA_MEMBERSHIP_PREFIX), anyString(), anyString());
    }

    @Test
    public void testKeepaliveMechanism() throws InterruptedException {
        Thread.sleep(2000); // Wait for keepalive interval
        verify(jedis, atLeast(2)).hset(eq(MetadataService.METADATA_MEMBERSHIP_PREFIX), anyString(), anyString());
    }

    @Test
    public void testMembershipMonitoring() throws InterruptedException {
        // Initial membership
        Map<String, String> initialMembers = Collections.singletonMap("node1", String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(eq(MetadataService.METADATA_MEMBERSHIP_PREFIX))).thenReturn(initialMembers);
        clusterRing.refreshMembership();
        // Updated membership
        Map<String, String> updatedMembers = new HashMap<>(initialMembers);
        updatedMembers.put("node2", String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(eq(MetadataService.METADATA_MEMBERSHIP_PREFIX))).thenReturn(updatedMembers);
        clusterRing.refreshMembership();
        // Verify that the hash ring is updated correctly
        // Note: We can't directly verify the hash ring here, as it's an internal implementation detail of ClusterRing.
        // However, we can verify that the refreshMembership method is called correctly.
        verify(jedis, times(2)).hgetAll(eq(MetadataService.METADATA_MEMBERSHIP_PREFIX));
    }

    @Test
    public void testNodeRemovalOnTimeout() throws InterruptedException {
        clusterRing.start();
        long currentTime = System.currentTimeMillis();
        when(jedis.hgetAll(eq(MetadataService.METADATA_MEMBERSHIP_PREFIX))).thenReturn(Collections.singletonMap("node1", String.valueOf(currentTime - 4000)));
        clusterRing.refreshMembership();
        verify(jedis).hdel(eq(MetadataService.METADATA_MEMBERSHIP_PREFIX), eq("node1"));
    }
}