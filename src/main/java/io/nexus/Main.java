package io.nexus;

import java.net.URI;
import java.util.Properties;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nexus.configuration.JCloudsConfig;
import io.nexus.configuration.ServerConfig;
import io.nexus.configuration.PropertiesLoader;
import io.nexus.configuration.RedisConfig;
import io.nexus.configuration.S3ProxyConfig;
import io.nexus.s3proxy.AuthenticationType;
import io.nexus.s3proxy.S3Proxy;
import io.nexus.streamlets.StreamletsInterceptor;
import io.nexus.streamlets.metadata.MetadataService;
import redis.clients.jedis.Jedis;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static volatile boolean running = true;
    private static final PropertiesLoader config = new PropertiesLoader();
    private static final ServerConfig webserver = new ServerConfig(config);

    public static void main(String[] args) throws Exception {

        // Initialize main configurations
        final JCloudsConfig JCLOUDS_CONFIG = new JCloudsConfig(config);
        final S3ProxyConfig S3PROXY_CONFIG = new S3ProxyConfig(config);
        final RedisConfig REDIS_CONFIG = new RedisConfig(config);
        logger.info("S3Proxy configuration {}", S3PROXY_CONFIG);
        logger.info("JClouds configuration {}", JCLOUDS_CONFIG);
        logger.info("Redis configuration {}", REDIS_CONFIG);

        // Configure and initialize JClouds with the object storage's details
        Properties objectStoreProperties = new Properties();
        objectStoreProperties.setProperty("jclouds.filesystem.basedir", JCLOUDS_CONFIG.getFilesystemBasedir());
        BlobStoreContext context = ContextBuilder.newBuilder(JCLOUDS_CONFIG.getProvider())
                .credentials(JCLOUDS_CONFIG.getIdentity(), JCLOUDS_CONFIG.getCredential())
                .overrides(objectStoreProperties).endpoint(JCLOUDS_CONFIG.getEndpoint()).build(BlobStoreContext.class);
        logger.info("Initialized object storage link");

        // Initializing the metadata service with the redis config
        MetadataService metadataService = new MetadataService(
                new Jedis(REDIS_CONFIG.getHost(), REDIS_CONFIG.getPort()));
        logger.info("Initialized metadata service");

        // Nexus interceptor middleware
        StreamletsInterceptor streamletsMiddleware = new StreamletsInterceptor(context.getBlobStore(), metadataService);
        logger.info("Initialized interceptor middleware");

        // Initialize S3Proxy with the streaming service's endpoint
        S3Proxy s3Proxy = S3Proxy.builder().blobStore(streamletsMiddleware)
                .endpoint(URI.create(S3PROXY_CONFIG.getEndpoint()))
                .awsAuthentication(AuthenticationType.NONE, null, null).build();

        s3Proxy.start();
        logger.info("Initialized S3 Proxy interceptor");

        while (running) {
            logger.info("Main server task is running...");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                logger.error("Error during main thread execution.", e);
                shutdownMainApplication();
            }
        }
        System.exit(0);
    }

    // Shuts down the main server and Spring Boot application gracefully
    private static void shutdownMainApplication() {
        System.out.println("Shutting down the main server due to an error in metadata server...");
        running = false; // This will terminate the main server loop
        System.exit(1); // Exit the application with a non-zero status
    }
}
