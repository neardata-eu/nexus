package io.nexus;

import java.net.URI;
import java.util.Properties;

import io.nexus.configuration.JCloudsConfig;
import io.nexus.configuration.NexusConfig;
import io.nexus.configuration.PropertiesLoader;
import io.nexus.configuration.RedisConfig;
import io.nexus.configuration.S3ProxyConfig;
import io.nexus.configuration.ServerConfig;

import io.nexus.streamlets.cluster.ClusterRing;
import io.nexus.streamlets.metadata.MetadataChangeNotifier;
import io.nexus.streamlets.state.StreamletStateBackendFactory;
import io.nexus.streamlets.state.StreamletStateManager;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nexus.s3proxy.AuthenticationType;
import io.nexus.s3proxy.S3Proxy;
import io.nexus.streamlets.StreamletsInterceptor;
import io.nexus.streamlets.metadata.MetadataService;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static volatile boolean running = true;
    private static final PropertiesLoader config = new PropertiesLoader();
    // Anonymously instantiating the metrics web server
    static {
        new ServerConfig(config);
    }

    public static void main(String[] args) throws Exception {
        // Initialize main configurations
        final JCloudsConfig JCLOUDS_CONFIG = new JCloudsConfig(config);
        final S3ProxyConfig S3PROXY_CONFIG = new S3ProxyConfig(config);
        // TODO: Check if notify-keyspace-events can be adjusted to AKE upon startup
        final RedisConfig REDIS_CONFIG = new RedisConfig(config);
        final NexusConfig NEXUS_CONFIG = new NexusConfig(config);
        logger.info("S3Proxy configuration {}", S3PROXY_CONFIG);
        logger.info("JClouds configuration {}", JCLOUDS_CONFIG);
        logger.info("Redis configuration {}", REDIS_CONFIG);
        logger.info("Nexus configuration {}", NEXUS_CONFIG);

        // Configure and initialize JClouds with the object storage's details
        Properties objectStoreProperties = new Properties();
        objectStoreProperties.setProperty("jclouds.filesystem.basedir", JCLOUDS_CONFIG.getFilesystemBasedir());
        BlobStoreContext context = ContextBuilder.newBuilder(JCLOUDS_CONFIG.getProvider())
                .credentials(JCLOUDS_CONFIG.getIdentity(), JCLOUDS_CONFIG.getCredential())
                .overrides(objectStoreProperties).endpoint(JCLOUDS_CONFIG.getEndpoint()).build(BlobStoreContext.class);
        logger.info("Initialized object storage link");

        // Initializing a Redis pool for metadata multithreaded interaction support
        JedisPoolConfig jedisConfig = new JedisPoolConfig();
        final JedisPool jedisPool = new JedisPool(jedisConfig, REDIS_CONFIG.getHost(), REDIS_CONFIG.getPort());
        MetadataChangeNotifier metadataChangeNotifier = new MetadataChangeNotifier(jedisPool);
        metadataChangeNotifier.initializeSubscriber();
        MetadataService metadataService = new MetadataService(NEXUS_CONFIG, jedisPool, metadataChangeNotifier);
        metadataChangeNotifier.registerCallback(metadataService);
        logger.info("Initialized metadata service");

        // Nexus interceptor middleware
        ClusterRing clusterRing = new ClusterRing(jedisPool, S3PROXY_CONFIG.getEndpoint(),
                NEXUS_CONFIG.getKeepaliveInterval(), NEXUS_CONFIG.getTimeout(), NEXUS_CONFIG.getClusterVirtualNodes());
        clusterRing.start();
        metadataChangeNotifier.registerCallback(clusterRing);
        StreamletStateManager stateManager = new StreamletStateManager(StreamletStateBackendFactory.createBackend(NEXUS_CONFIG));
        StreamletsInterceptor streamletsMiddleware = new StreamletsInterceptor(context.getBlobStore(), metadataService, stateManager, clusterRing);
        logger.info("Initialized interceptor middleware");

        // Initialize S3Proxy with the streaming service's endpoint
        S3Proxy s3Proxy = S3Proxy.builder().blobStore(streamletsMiddleware)
                .endpoint(URI.create(S3PROXY_CONFIG.getEndpoint()))
                .awsAuthentication(AuthenticationType.NONE, null, null)
                .ignoreUnknownHeaders(true)
                .build();

        s3Proxy.start();
        logger.info("Initialized S3 Proxy interceptor");

        while (running) {
            logger.info("Main server task is running...");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                logger.error("Error during main thread execution.", e);
                // Shutdown the redis pool gracefully
                jedisPool.close();
                metadataChangeNotifier.close();
                shutdownMainApplication();
            }
        }
        System.exit(0);
    }

    // Shuts down the main server gracefully
    private static void shutdownMainApplication() {
        System.out.println("Shutting down the main server due to an error in metadata server...");
        running = false; // This will terminate the main server loop
        System.exit(1); // Exit the application with a non-zero status
    }
}
