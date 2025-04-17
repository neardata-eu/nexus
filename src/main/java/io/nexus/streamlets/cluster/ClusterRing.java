package io.nexus.streamlets.cluster;

import io.nexus.streamlets.metadata.MetadataCallback;
import io.nexus.streamlets.metadata.MetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.URI;
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
    private final String serviceEndpoint;
    private final int servicePort;
    private final HashRing hashRing;
    private final AtomicReference<String> thisNodeHostId = new AtomicReference<>();
    private volatile Set<String> previousMembers = new HashSet<>();

    /**
     * Constructs a new ClusterRing instance.
     *
     * @param jedisPool the JedisPool instance to use for Redis connections
     * @param serviceEndpoint the endpoint to use for the service
     * @param keepaliveInterval the interval at which to send keepalive messages
     * @param timeout the timeout after which a node is considered dead
     * @param virtualNodes the number of virtual nodes per physical node
     */
    public ClusterRing(JedisPool jedisPool, String serviceEndpoint, int keepaliveInterval, int timeout, int virtualNodes) {
        this.jedisPool = jedisPool;
        this.serviceEndpoint = serviceEndpoint;
        // Hosts are organized as MembershipKey:SwarmletServiceName -> [WorkerNodeEndpoint]
        this.membershipKey = MetadataService.METADATA_MEMBERSHIP_PREFIX + URI.create(this.serviceEndpoint).getHost();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.keepaliveInterval = keepaliveInterval;
        this.timeout = timeout;
        this.servicePort = URI.create(serviceEndpoint).getPort();
        try {
            this.hashRing = new HashRing(virtualNodes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the owner node endpoint for a given key in this swarmlet.
     *
     * @param key the key to map
     * @return the node that the key maps to
     */
    public String getNodeForKey(String key) {
        return this.hashRing.getNode(key);
    }

    /**
     * Registers a node with the cluster.
     */
    private void registerNode() {
        try (Jedis jedis = jedisPool.getResource()) {
            String nodeKey = InetAddress.getLocalHost().getHostName() + ":" + servicePort;
            this.thisNodeHostId.set(nodeKey);
            jedis.hset(this.membershipKey, nodeKey, String.valueOf(System.currentTimeMillis()));
            logger.info("Registering this node in metadata service: {} : {}", this.membershipKey, nodeKey);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts the keepalive mechanism.
     */
    private void startKeepAlive() {
        scheduler.scheduleAtFixedRate(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                String nodeKey = InetAddress.getLocalHost().getHostName() + ":" + servicePort;
                if (!nodeKey.equals(getThisNodeHostId())) {
                    logger.warn("Host key for this node has changed: {} -> {}", getThisNodeHostId(), nodeKey);
                }
                this.thisNodeHostId.set(nodeKey);
                jedis.hset(this.membershipKey, nodeKey, String.valueOf(System.currentTimeMillis()));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }, keepaliveInterval, keepaliveInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Starts monitoring membership changes.
     */
    @Override
    public void onMessage(String key, String message) {
        logger.debug("Received Redis event: {} on key: {}", message, key);
        if (key.equals(membershipKey)) {
            refreshMembership();
        }
    }

    /**
     * Refreshes the membership information and updates the HashRing only if there is a real change.
     */
    void refreshMembership() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> currentMembers = jedis.hgetAll(membershipKey);
            long now = System.currentTimeMillis();
            for (String node : currentMembers.keySet()) {
                long timestamp = Long.parseLong(currentMembers.get(node));
                if (now - timestamp > timeout) {
                    jedis.hdel(membershipKey, node);
                }
            }
            Set<String> updatedMembers = new HashSet<>(currentMembers.keySet());
            if (!updatedMembers.equals(previousMembers)) {
                hashRing.updateNodes(new ArrayList<>(updatedMembers));
                previousMembers = updatedMembers;
                logger.info("Hash ring updated: {}", hashRing);
            }
        }
    }

    /**
     * Starts the cluster ring.
     */
    public void start() {
        registerNode();
        refreshMembership();  // Ensure HashRing is populated at startup
        startKeepAlive();
    }

    /**
     * Closes the cluster ring and shuts down the scheduler.
     */
    @Override
    public void close() {
        scheduler.shutdown();
    }

    /**
     * Retrieves the current membership information from Redis.
     *
     * @return the current membership information
     */
    public Map<String, String> getMembership() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll(membershipKey);
        }
    }

    public String getThisNodeHostId() {
        return thisNodeHostId.get();
    }
}