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

import static org.junit.jupiter.api.Assertions.*;

public class ClusterRingTest {
    private JedisPool jedisPool;
    private Jedis jedis;
    private ClusterRing clusterRing;
    private String expectedMembershipKey;

    @BeforeEach
    public void setUp() throws Exception {
        jedisPool = mock(JedisPool.class);
        jedis = mock(Jedis.class);
        when(jedisPool.getResource()).thenReturn(jedis);
        doNothing().when(jedis).subscribe(any(JedisPubSub.class), anyString());
        doNothing().when(jedis).close(); // Mock close() method

        // Calculate the expected membership key based on the service endpoint
        expectedMembershipKey = MetadataService.METADATA_MEMBERSHIP_PREFIX + "nexus-svc" + "-" + "EDGE";

        // Mock empty initial membership
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(new HashMap<>());

        clusterRing = new ClusterRing(jedisPool, "nexus-svc" + "-" + "EDGE", 8080, 1000, 3000, 10);
    }

    @AfterEach
    public void tearDown() {
        if (clusterRing != null && !clusterRing.isShutdown()) {
            clusterRing.close();
        }
        reset(jedis, jedisPool);
    }

    @Test
    public void testNodeRegistration() {
        // Start the cluster ring
        clusterRing.start();

        // Verify that hset was called with the correct membership key
        verify(jedis, atLeastOnce()).hset(eq(expectedMembershipKey), anyString(), anyString());
    }

    @Test
    public void testKeepaliveMechanism() throws InterruptedException {
        clusterRing.start();

        // Wait for at least one keepalive interval (1000ms + buffer)
        Thread.sleep(1500);

        // Verify multiple calls to hset (initial registration + keepalive(s))
        verify(jedis, atLeast(2)).hset(eq(expectedMembershipKey), anyString(), anyString());
    }

    @Test
    public void testMembershipMonitoring() {
        clusterRing.start();

        // Reset mock to clear initial calls
        reset(jedis);
        when(jedisPool.getResource()).thenReturn(jedis);
        doNothing().when(jedis).close();

        // Test initial membership
        Map<String, String> initialMembers = Collections.singletonMap("node1:8080", String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(initialMembers);

        clusterRing.refreshMembership();

        // Test updated membership
        Map<String, String> updatedMembers = new HashMap<>(initialMembers);
        updatedMembers.put("node2:8080", String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(updatedMembers);

        clusterRing.refreshMembership();

        // Verify that hgetAll was called with correct key
        verify(jedis, times(2)).hgetAll(eq(expectedMembershipKey));

        // Verify we can get nodes for keys after membership changes
        String nodeForKey = clusterRing.getNodeForKey("testkey");
        // Should return one of the nodes or null if no nodes
        assertTrue(nodeForKey == null || updatedMembers.containsKey(nodeForKey) ||
                nodeForKey.equals(clusterRing.getThisNodeHostId()));
    }

    @Test
    public void testNodeRemovalOnTimeout() {
        clusterRing.start();

        // Reset mock to clear initial calls
        reset(jedis);
        when(jedisPool.getResource()).thenReturn(jedis);
        doNothing().when(jedis).close();

        // Create expired membership (4000ms ago, timeout is 3000ms)
        long expiredTime = System.currentTimeMillis() - 4000;
        Map<String, String> expiredMembers = Collections.singletonMap("node1:8080", String.valueOf(expiredTime));
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(expiredMembers);

        clusterRing.refreshMembership();

        // Verify that expired node was removed
        verify(jedis).hdel(eq(expectedMembershipKey), eq("node1:8080"));
    }

    @Test
    public void testGetNodeForKey() {
        clusterRing.start();

        // Add some test membership
        Map<String, String> members = new HashMap<>();
        members.put("node1:8080", String.valueOf(System.currentTimeMillis()));
        members.put("node2:8080", String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(members);

        clusterRing.refreshMembership();

        // Test key mapping
        String node1 = clusterRing.getNodeForKey("test-key-1");
        String node2 = clusterRing.getNodeForKey("test-key-2");

        // Should return valid nodes (including this node or members)
        assertNotNull(node1);
        assertNotNull(node2);

        // Same key should always map to same node
        assertEquals(node1, clusterRing.getNodeForKey("test-key-1"));
        assertEquals(node2, clusterRing.getNodeForKey("test-key-2"));

        // Null key should return null
        assertNull(clusterRing.getNodeForKey(null));
    }

    @Test
    public void testGetMembership() {
        Map<String, String> expectedMembership = new HashMap<>();
        expectedMembership.put("node1:8080", String.valueOf(System.currentTimeMillis()));
        expectedMembership.put("node2:8080", String.valueOf(System.currentTimeMillis()));

        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(expectedMembership);

        clusterRing.start();
        Map<String, String> actualMembership = clusterRing.getMembership();

        assertEquals(expectedMembership, actualMembership);
    }

    @Test
    public void testGetActiveNodes() {
        // Setup mock to return this node's registration when refreshMembership is called
        Map<String, String> thisNodeMembership = new HashMap<>();
        thisNodeMembership.put(clusterRing.getThisNodeHostId(), String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(thisNodeMembership);

        clusterRing.start();

        // Should contain this node after start
        Set<String> initialNodes = clusterRing.getActiveNodes();
        assertTrue(initialNodes.contains(clusterRing.getThisNodeHostId()), "Active nodes should contain this node: " +
                        clusterRing.getThisNodeHostId() + ", but got: " + initialNodes);

        // Add more members
        Map<String, String> members = new HashMap<>();
        members.put(clusterRing.getThisNodeHostId(), String.valueOf(System.currentTimeMillis())); // Keep this node
        members.put("node1:8080", String.valueOf(System.currentTimeMillis()));
        members.put("node2:8080", String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(members);

        clusterRing.refreshMembership();

        Set<String> activeNodes = clusterRing.getActiveNodes();
        assertTrue(activeNodes.contains("node1:8080"));
        assertTrue(activeNodes.contains("node2:8080"));
        assertTrue(activeNodes.contains(clusterRing.getThisNodeHostId()));
    }

    @Test
    public void testShutdown() {
        clusterRing.start();
        assertFalse(clusterRing.isShutdown());

        clusterRing.close();
        assertTrue(clusterRing.isShutdown());

        // Should unregister this node on shutdown
        verify(jedis, atLeastOnce()).hdel(eq(expectedMembershipKey), eq(clusterRing.getThisNodeHostId()));
    }

    @Test
    public void testMembershipWithInvalidTimestamp() {
        clusterRing.start();

        // Reset mock to clear initial calls
        reset(jedis);
        when(jedisPool.getResource()).thenReturn(jedis);
        doNothing().when(jedis).close();

        // Create membership with invalid timestamp
        Map<String, String> invalidMembers = Collections.singletonMap("node1:8080", "invalid-timestamp");
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(invalidMembers);

        clusterRing.refreshMembership();

        // Should treat invalid timestamp as expired and remove the node
        verify(jedis).hdel(eq(expectedMembershipKey), eq("node1:8080"));
    }

    @Test
    public void testConcurrentMembershipRefresh() throws InterruptedException {
        clusterRing.start();

        Map<String, String> members = Collections.singletonMap("node1:8080", String.valueOf(System.currentTimeMillis()));
        when(jedis.hgetAll(expectedMembershipKey)).thenReturn(members);

        // Start multiple threads calling refreshMembership
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> clusterRing.refreshMembership());
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(1000);
        }

        // Should not throw any exceptions and should have called hgetAll multiple times
        verify(jedis, atLeast(5)).hgetAll(expectedMembershipKey);
    }
}