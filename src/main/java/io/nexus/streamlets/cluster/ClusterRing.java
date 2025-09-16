package io.nexus.streamlets.cluster;

import io.nexus.streamlets.metadata.MetadataCallback;
import io.nexus.streamlets.metadata.MetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A ClusterRing implementation that manages a cluster of nodes and provides a way to register nodes, start the
 * keepalive mechanism, and monitor membership changes. This class uses a HashRing to map keys to nodes in the cluster.
 */
public class ClusterRing implements MetadataCallback, Closeable {
    private final Logger logger = LoggerFactory.getLogger(ClusterRing.class);
    private final JedisPool jedisPool;
    private final String membershipKey;
    private final ScheduledExecutorService scheduler;
    private final int keepaliveInterval;
    private final int timeout;
    private final int servicePort;
    private final HashRing hashRing;
    private final AtomicReference<String> thisNodeHostId = new AtomicReference<>();
    private volatile Set<String> previousMembers = new HashSet<>();

    // Add synchronization for membership operations
    private final Object membershipLock = new Object();

    // Cache the node key to avoid repeated hostname resolution
    private volatile String cachedNodeKey;
    private volatile boolean isShutdown = false;

    /**
     * Constructs a new ClusterRing instance.
     *
     * @param jedisPool the JedisPool instance to use for Redis connections
     * @param ringId the endpoint to use for the service
     * @param servicePort service port for nodes
     * @param keepaliveInterval the interval at which to send keepalive messages
     * @param timeout the timeout after which a node is considered dead
     * @param virtualNodes the number of virtual nodes per physical node
     */
    public ClusterRing(JedisPool jedisPool, String ringId, int servicePort, int keepaliveInterval, int timeout, int virtualNodes) {
        this.jedisPool = jedisPool;
        // Hosts are organized as MembershipKey:Swarmlet-Region -> [WorkerNodeEndpoint]
        this.membershipKey = MetadataService.METADATA_MEMBERSHIP_PREFIX + ringId;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.keepaliveInterval = keepaliveInterval;
        this.timeout = timeout;
        this.servicePort = servicePort;
        try {
            this.hashRing = new HashRing(virtualNodes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to create HashRing", e);
        }

        // Initialize node key once at construction
        initializeNodeKey();
    }

    /**
     * Initializes and caches the node key to avoid repeated hostname resolution.
     */
    private void initializeNodeKey() {
        try {
            this.cachedNodeKey = InetAddress.getLocalHost().getHostName() + ":" + servicePort;
            this.thisNodeHostId.set(this.cachedNodeKey);
            logger.info("Initialized node key: {}", this.cachedNodeKey);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to resolve local hostname", e);
        }
    }

    /**
     * Gets the cached node key, with fallback to re-resolution if needed.
     */
    private String getNodeKey() {
        String nodeKey = this.cachedNodeKey;
        if (nodeKey == null) {
            synchronized (this) {
                nodeKey = this.cachedNodeKey;
                if (nodeKey == null) {
                    try {
                        nodeKey = InetAddress.getLocalHost().getHostName() + ":" + servicePort;
                        this.cachedNodeKey = nodeKey;
                        this.thisNodeHostId.set(nodeKey);
                        logger.info("Re-resolved node key: {}", nodeKey);
                    } catch (UnknownHostException e) {
                        logger.error("Failed to resolve hostname, using previous key: {}", this.thisNodeHostId.get());
                        return this.thisNodeHostId.get();
                    }
                }
            }
        }
        return nodeKey;
    }

    /**
     * Returns the owner node endpoint for a given key in this swarmlet.
     *
     * @param key the key to map
     * @return the node that the key maps to
     */
    public String getNodeForKey(String key) {
        if (key == null) {
            return null;
        }
        String node = this.hashRing.getNode(key);
        logger.info(visualizeRing(key, node));
        return node;
    }

    /**
     * Returns a compact view of the hash ring with node distribution.
     */
    private String visualizeRing(String key, String assignedNode) {
        long keyHash = hashRing.hash(key);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Key '%s' hashes to %d and maps to node '%s'\n", key, keyHash, assignedNode));
        sb.append("Hash Ring:\n");

        // Get a snapshot of the ring and iterate in order
        Map<Long, String> ringView = hashRing.getRingView(); // Assuming this returns sorted Map<Long, String>
        Long prev = null;
        for (Map.Entry<Long, String> entry : ringView.entrySet()) {
            if (prev != null) {
                sb.append(String.format("[%d, %d) → %s\n", prev, entry.getKey(), entry.getValue()));
            }
            prev = entry.getKey();
        }

        // Wrap-around interval (last → first)
        if (!ringView.isEmpty()) {
            Long firstKey = ringView.keySet().iterator().next();
            sb.append(String.format("[%d, MAX] ∪ [0, %d) → %s\n",
                    prev, firstKey, ringView.get(firstKey)));
        }

        return sb.toString();
    }

    /**
     * Registers a node with the cluster.
     */
    private void registerNode() {
        if (isShutdown) {
            return;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            String nodeKey = getNodeKey();
            jedis.hset(this.membershipKey, nodeKey, String.valueOf(System.currentTimeMillis()));
            logger.info("Registered node in metadata service: {} : {}", this.membershipKey, nodeKey);
        } catch (Exception e) {
            logger.error("Failed to register node", e);
            throw new RuntimeException("Failed to register node", e);
        }
    }

    /**
     * Starts the keepalive mechanism.
     */
    private void startKeepAlive() {
        scheduler.scheduleAtFixedRate(() -> {
            if (isShutdown) {
                return;
            }

            try (Jedis jedis = jedisPool.getResource()) {
                String nodeKey = getNodeKey();
                jedis.hset(this.membershipKey, nodeKey, String.valueOf(System.currentTimeMillis()));
                logger.debug("Sent keepalive for node: {}", nodeKey);
            } catch (Exception e) {
                logger.error("Failed to send keepalive", e);
                // Don't throw here as it would stop the scheduled task
            }
        }, keepaliveInterval, keepaliveInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Handles Redis keyspace notifications for membership changes.
     */
    @Override
    public void onMessage(String key, String message) {
        if (isShutdown) {
            return;
        }

        logger.debug("Received Redis event: {} on key: {}", message, key);
        if (key.equals(membershipKey)) {
            refreshMembership();
        }
    }

    /**
     * Refreshes the membership information and updates the HashRing only if there is a real change.
     * This method is thread-safe and handles expired node cleanup correctly.
     */
    void refreshMembership() {
        if (isShutdown) {
            return;
        }

        synchronized (membershipLock) {
            try (Jedis jedis = jedisPool.getResource()) {
                // Get current membership from Redis
                Map<String, String> currentMembers = jedis.hgetAll(membershipKey);
                if (currentMembers == null) {
                    currentMembers = new HashMap<>();
                }

                long now = System.currentTimeMillis();
                Set<String> expiredNodes = new HashSet<>();
                Set<String> validNodes = new HashSet<>();

                // Process each member to identify expired vs valid nodes
                for (Map.Entry<String, String> entry : currentMembers.entrySet()) {
                    String node = entry.getKey();
                    String timestampStr = entry.getValue();

                    try {
                        long timestamp = Long.parseLong(timestampStr);
                        if (now - timestamp > timeout) {
                            expiredNodes.add(node);
                            logger.info("Node {} expired (last seen {} ms ago)", node, now - timestamp);
                        } else {
                            validNodes.add(node);
                        }
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid timestamp for node {}: {}, treating as expired", node, timestampStr);
                        expiredNodes.add(node);
                    }
                }

                // Remove expired nodes from Redis
                if (!expiredNodes.isEmpty()) {
                    String[] expiredArray = expiredNodes.toArray(new String[0]);
                    jedis.hdel(membershipKey, expiredArray);
                    logger.info("Removed {} expired nodes from Redis: {}", expiredNodes.size(), expiredNodes);
                }

                // Update hash ring only if membership actually changed
                if (!validNodes.equals(previousMembers)) {
                    if (validNodes.isEmpty()) {
                        logger.warn("No valid nodes found in cluster membership");
                    }

                    hashRing.updateNodes(new ArrayList<>(validNodes));
                    previousMembers = new HashSet<>(validNodes); // Create defensive copy
                    logger.info("Hash ring updated with {} nodes: {}", validNodes.size(), validNodes);
                    logger.debug("Hash ring state: {}", hashRing);
                } else {
                    logger.debug("No membership changes detected");
                }

            } catch (Exception e) {
                logger.error("Failed to refresh membership", e);
                // Don't rethrow - we want to continue operating with current membership
            }
        }
    }

    /**
     * Starts the cluster ring.
     */
    public void start() {
        if (isShutdown) {
            throw new IllegalStateException("Cannot start a shutdown ClusterRing");
        }

        logger.info("Starting ClusterRing for service: {}", this.membershipKey);
        registerNode();
        refreshMembership();  // Ensure HashRing is populated at startup
        startKeepAlive();
        logger.info("ClusterRing started successfully");
    }

    /**
     * Closes the cluster ring and shuts down the scheduler.
     */
    @Override
    public void close() {
        if (isShutdown) {
            return;
        }

        logger.info("Shutting down ClusterRing");
        isShutdown = true;

        // Unregister this node from the cluster
        try (Jedis jedis = jedisPool.getResource()) {
            String nodeKey = getNodeKey();
            jedis.hdel(membershipKey, nodeKey);
            logger.info("Unregistered node from cluster: {}", nodeKey);
        } catch (Exception e) {
            logger.error("Failed to unregister node during shutdown", e);
        }

        // Shutdown scheduler gracefully
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Scheduler did not terminate gracefully");
                }
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("ClusterRing shutdown complete");
    }

    /**
     * Retrieves the current membership information from Redis.
     *
     * @return the current membership information
     */
    public Map<String, String> getMembership() {
        if (isShutdown) {
            return new HashMap<>();
        }

        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> membership = jedis.hgetAll(membershipKey);
            return membership != null ? membership : new HashMap<>();
        } catch (Exception e) {
            logger.error("Failed to retrieve membership", e);
            return new HashMap<>();
        }
    }

    /**
     * Gets the host ID for this node.
     *
     * @return the host ID of this node
     */
    public String getThisNodeHostId() {
        return thisNodeHostId.get();
    }

    /**
     * Gets the current active nodes in the hash ring.
     *
     * @return a copy of the current active nodes
     */
    public Set<String> getActiveNodes() {
        synchronized (membershipLock) {
            return new HashSet<>(previousMembers);
        }
    }

    /**
     * Checks if the cluster ring is shutdown.
     *
     * @return true if shutdown, false otherwise
     */
    public boolean isShutdown() {
        return isShutdown;
    }
}