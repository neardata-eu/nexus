package io.nexus.streamlets.cluster;

import com.google.common.annotations.VisibleForTesting;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A HashRing implementation that uses a DHT-like assignment of virtual nodes to system nodes.
 * This class provides a way to map a key to a node in the hash ring, and to update the nodes in the hash ring.
 */
public class HashRing {
    @VisibleForTesting
    final SortedMap<Long, String> hashRing;
    private final ThreadLocal<MessageDigest> md5ThreadLocal;
    private final int virtualNodesPerNode;

    /**
     * Constructs a new HashRing instance.
     *
     * @param virtualNodesPerNode the number of virtual nodes per physical node
     * @throws java.security.NoSuchAlgorithmException if the MD5 message digest algorithm is not available
     */
    public HashRing(int virtualNodesPerNode) throws NoSuchAlgorithmException {
        this.hashRing = Collections.synchronizedSortedMap(new TreeMap<>());
        this.md5ThreadLocal = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        });
        this.virtualNodesPerNode = virtualNodesPerNode;
    }

    /**
     * Adds a node to the hash ring.
     *
     * @param node the node to add
     */
    public void addNode(String node) {
        if (node == null || node.trim().isEmpty()) {
            return;
        }

        synchronized (hashRing) {
            for (int i = 0; i < virtualNodesPerNode; i++) {
                String virtualNode = node + ":" + i;
                long hash = hash(virtualNode);

                // Handle hash collisions by using linear probing
                while (hashRing.containsKey(hash)) {
                    hash = (hash + 1) & Long.MAX_VALUE; // Keep positive
                }

                hashRing.put(hash, node);
            }
        }
    }

    /**
     * Removes a node from the hash ring.
     *
     * @param node the node to remove
     */
    public void removeNode(String node) {
        if (node == null || node.trim().isEmpty()) {
            return;
        }

        synchronized (hashRing) {
            for (int i = 0; i < virtualNodesPerNode; i++) {
                String virtualNode = node + ":" + i;
                long hash = hash(virtualNode);

                // Find the actual hash used (in case of collision resolution)
                while (hashRing.containsKey(hash)) {
                    if (node.equals(hashRing.get(hash))) {
                        hashRing.remove(hash);
                        break;
                    }
                    hash = (hash + 1) & Long.MAX_VALUE;
                }
            }
        }
    }

    /**
     * Maps a key to a node in the hash ring.
     *
     * @param key the key to map
     * @return the node that the key maps to, or null if no nodes exist
     */
    public String getNode(String key) {
        if (key == null) {
            return null;
        }

        long hash = hash(key);
        synchronized (hashRing) {
            if (hashRing.isEmpty()) {
                return null;
            }

            SortedMap<Long, String> tail = hashRing.tailMap(hash);
            if (tail.isEmpty()) {
                return hashRing.get(hashRing.firstKey());
            } else {
                return tail.get(tail.firstKey());
            }
        }
    }

    /**
     * Updates the nodes in the hash ring.
     *
     * @param nodes the new nodes to use
     */
    public void updateNodes(List<String> nodes) {
        if (nodes == null) {
            return;
        }

        synchronized (hashRing) {
            hashRing.clear();
            for (String node : nodes) {
                addNode(node);
            }
        }
    }

    /**
     * Computes the hash value of a string using all 16 bytes of MD5.
     *
     * @param key the string to hash
     * @return the hash value of the string
     */
    @VisibleForTesting
    long hash(String key) {
        MessageDigest md5 = md5ThreadLocal.get();
        md5.reset(); // Reset for clean state
        byte[] digest = md5.digest(key.getBytes());

        // Use all 16 bytes of MD5, taking first 8 bytes for long
        long hash = 0;
        for (int i = 0; i < 8 && i < digest.length; i++) {
            hash = (hash << 8) | (digest[i] & 0xFF);
        }
        return hash & Long.MAX_VALUE; // Keep positive to avoid issues with tailMap
    }

    @Override
    public String toString() {
        synchronized (hashRing) {
            Map<String, Long> distribution = this.hashRing.entrySet().stream()
                    .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.counting()));
            return distribution.entrySet().stream()
                    .map(e -> String.format("%s → %d slots", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(", ", "[", "]"));
        }
    }

    /**
     * Returns a snapshot view of the current hash ring for diagnostics.
     *
     * @return a sorted, unmodifiable copy of the hash ring (hash -> node)
     */
    public SortedMap<Long, String> getRingView() {
        synchronized (hashRing) {
            return Collections.unmodifiableSortedMap(new TreeMap<>(hashRing));
        }
    }
}