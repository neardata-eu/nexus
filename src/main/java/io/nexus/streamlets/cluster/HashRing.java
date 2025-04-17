package io.nexus.streamlets.cluster;

import com.google.common.annotations.VisibleForTesting;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * A HashRing implementation that uses a DHT-like assignment of virtual nodes to system nodes.
 * This class provides a way to map a key to a node in the hash ring, and to update the nodes in the hash ring.
 */
public class HashRing {
    @VisibleForTesting
    final SortedMap<Long, String> hashRing;
    private final MessageDigest md5;
    private final int virtualNodesPerNode;

    /**
     * Constructs a new HashRing instance.
     *
     * @param virtualNodesPerNode the number of virtual nodes per physical node
     * @throws java.security.NoSuchAlgorithmException if the MD5 message digest algorithm is not available
     */
    public HashRing(int virtualNodesPerNode) throws NoSuchAlgorithmException {
        this.hashRing = Collections.synchronizedSortedMap(new TreeMap<>());
        this.md5 = MessageDigest.getInstance("MD5");
        this.virtualNodesPerNode = virtualNodesPerNode;
    }

    /**
     * Adds a node to the hash ring.
     *
     * @param node the node to add
     */
    public void addNode(String node) {
        for (int i = 0; i < virtualNodesPerNode; i++) {
            String virtualNode = node + ":" + i;
            long hash = hash(virtualNode);
            hashRing.put(hash, node);
        }
    }

    /**
     * Removes a node from the hash ring.
     *
     * @param node the node to remove
     */
    public void removeNode(String node) {
        for (int i = 0; i < virtualNodesPerNode; i++) {
            String virtualNode = node + ":" + i;
            long hash = hash(virtualNode);
            hashRing.remove(hash);
        }
    }

    /**
     * Maps a key to a node in the hash ring.
     *
     * @param key the key to map
     * @return the node that the key maps to
     */
    public String getNode(String key) {
        long hash = hash(key);
        synchronized (hashRing) {
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
        synchronized (hashRing) {
            hashRing.clear();
            for (String node : nodes) {
                addNode(node);
            }
        }
    }

    /**
     * Computes the hash value of a string.
     *
     * @param key the string to hash
     * @return the hash value of the string
     */
    private long hash(String key) {
        byte[] digest = md5.digest(key.getBytes());
        long hash = 0;
        for (byte b : digest) {
            hash = (hash << 8) | (b & 0xFF);
        }
        return hash;
    }

    @Override
    public String toString() {
        synchronized (hashRing) {
            return hashRing.entrySet().stream()
                    .map(entry -> entry.getKey() + "=>" + entry.getValue())
                    .collect(Collectors.joining(", ", "{", "}"));
        }
    }

}