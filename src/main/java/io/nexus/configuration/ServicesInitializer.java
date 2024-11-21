package io.nexus.configuration;

import java.net.URI;
import java.util.Properties;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import io.nexus.s3proxy.AuthenticationType;
import io.nexus.s3proxy.S3Proxy;
import io.nexus.streamlets.StreamletsInterceptor;
import io.nexus.streamlets.metadata.MetadataService;
import redis.clients.jedis.Jedis;

/**
 * This class initializes all Nexus's service dependencies by
 * overriding Spring CommandLineRunner's "run" method
 * and constructing objects for those dependencies upon running,
 * using the environment variables provided
 */
@Component
public class ServicesInitializer implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(ServicesInitializer.class);

    private final S3ProxyConfig s3ProxyConfig;
    private final JCloudsConfig jcloudsConfig;
    private final RedisConfig redisConfig;

    // Constructor for creating configuration objects from the environment variables
    // provided upon application startup
    public ServicesInitializer(S3ProxyConfig s3ProxyConfig, JCloudsConfig jcloudsConfig, RedisConfig redisConfig) {
        this.s3ProxyConfig = s3ProxyConfig;
        this.jcloudsConfig = jcloudsConfig;
        this.redisConfig = redisConfig;
        logger.info("S3Proxy configuration {}", s3ProxyConfig);
        logger.info("JClouds configuration {}", jcloudsConfig);
        logger.info("Redis configuration {}", redisConfig);
    }

    // CommandLineRunner "run" method override to initialize Nexus with the
    // constructed configuration objects
    @Override
    public void run(String... args) throws Exception {

        // Loading Nexus properties with JClouds
        Properties properties = new Properties();
        properties.setProperty("jclouds.filesystem.basedir", jcloudsConfig.getFilesystemBasedir());
        logger.info("Loading Nexus configuration {}", properties);

        // Configure and initialize S3 proxy interceptor
        BlobStoreContext context = ContextBuilder
                .newBuilder(jcloudsConfig.getProvider())
                .credentials(jcloudsConfig.getIdentity(), jcloudsConfig.getCredential())
                .overrides(properties).endpoint(jcloudsConfig.getEndpoint())
                .build(BlobStoreContext.class);

        // Initializing the metadata service with the redis config
        MetadataService metadataService = new MetadataService(new Jedis(redisConfig.getHost(), redisConfig.getPort()));

        // Nexus interceptor middleware
        StreamletsInterceptor streamletsMiddleware = new StreamletsInterceptor(context.getBlobStore(),
                metadataService);

        S3Proxy s3Proxy = S3Proxy.builder()
                .blobStore(streamletsMiddleware)
                .endpoint(URI.create(s3ProxyConfig.getEndpoint()))
                .awsAuthentication(AuthenticationType.NONE, null, null)
                .build();

        s3Proxy.start();
        logger.info("Initialized S3 Proxy interceptor");

    };
}
