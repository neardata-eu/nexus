package io.nexus.streamlets.cluster;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashRingTest {
    private HashRing hashRing;

    @BeforeEach
    public void setUp() throws Exception {
        hashRing = new HashRing(10); // Use a default number of virtual nodes per node
    }

    @Test
    public void testAddNode() {
        String node1 = "node1";
        String node2 = "node2";
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        Assert.assertEquals(20, hashRing.hashRing.size());
    }

    @Test
    public void testRemoveNode() {
        String node1 = "node1";
        String node2 = "node2";
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.removeNode(node1);
        Assertions.assertEquals(10, hashRing.hashRing.size());
    }

    @Test
    public void testGetNode() {
        String node1 = "node1";
        String node2 = "node2";
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        String key1 = "key1";
        String key2 = "key2";
        String node1Key = hashRing.getNode(key1);
        String node2Key = hashRing.getNode(key2);
        Assertions.assertTrue(node1Key.equals(node1) || node1Key.equals(node2));
        Assertions.assertTrue(node2Key.equals(node1) || node2Key.equals(node2));
    }

    @Test
    public void testUpdateNodes() {
        String node1 = "node1";
        String node2 = "node2";
        String node3 = "node3";
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        List<String> updatedNodes = Arrays.asList(node1, node3);
        hashRing.updateNodes(updatedNodes);
        Assertions.assertEquals(20, hashRing.hashRing.size());
    }

    @Test
    public void testKeyDistribution() {
        String node1 = "node1";
        String node2 = "node2";
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        Map<String, Integer> keyDistribution = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String node = hashRing.getNode(key);
            keyDistribution.put(node, keyDistribution.getOrDefault(node, 0) + 1);
        }
        Assert.assertEquals(2, keyDistribution.size());
        Assertions.assertTrue(keyDistribution.get(node1) > 0);
        Assertions.assertTrue(keyDistribution.get(node2) > 0);
    }

    @Test
    public void testVirtualNodeDistribution() {
        String node1 = "node1";
        String node2 = "node2";
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        Map<String, Integer> virtualNodeDistribution = new HashMap<>();
        for (Long hash : hashRing.hashRing.keySet()) {
            String node = hashRing.hashRing.get(hash);
            virtualNodeDistribution.put(node, virtualNodeDistribution.getOrDefault(node, 0) + 1);
        }
        Assertions.assertEquals(2, virtualNodeDistribution.size());
        Assertions.assertTrue(virtualNodeDistribution.get(node1) > 0);
        Assertions.assertTrue(virtualNodeDistribution.get(node2) > 0);
    }
}