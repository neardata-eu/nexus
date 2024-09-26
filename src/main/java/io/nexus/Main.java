package io.nexus;

import io.nexus.s3proxy.S3Proxy;
import io.nexus.streamlets.StreamletsInterceptor;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.MetadataServiceRunner;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import redis.clients.jedis.Jedis;

import java.net.URI;
import java.util.Properties;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {

        // Start the Nexus metadata service in a separate thread
        Thread springBootThread = new Thread(() -> {
            try {
                SpringApplication.run(MetadataServiceRunner.class, args);
            } catch (Exception e) {
                logger.error("Error during metadata service execution.", e);
                // Trigger the shutdown of the main application
                shutdownMainApplication();
            }
        });
        springBootThread.start();
        logger.info("Initialized Metadata Service.");

        // Load Nexus configuration.
        Properties properties = new Properties();
        properties.setProperty("jclouds.filesystem.basedir", "/tmp/blobstore");
        logger.info("Loading Nexus configuration {}", properties);

        // Configure and initialize S3 proxy interceptor.
        BlobStoreContext context = ContextBuilder
                .newBuilder("filesystem")
                .credentials("identity", "credential")
                .overrides(properties)
                .build(BlobStoreContext.class);
        // TODO: Fix this with a proper factory and configuration
        MetadataService metadataService = new MetadataService(new Jedis("localhost", 6379));
        StreamletsInterceptor streamletsMiddleware =  new StreamletsInterceptor(context.getBlobStore(),
                metadataService);
        S3Proxy s3Proxy = S3Proxy.builder()
                .blobStore(streamletsMiddleware)
                .endpoint(URI.create("http://127.0.0.1:8181"))
                .build();
        s3Proxy.start();
        logger.info("Initialized S3 Proxy interceptor.");

        while (running && s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
            logger.info("Main server task is running..." + s3Proxy.getState());
            try {
                Thread.sleep(30000); // Simulate some task
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
        running = false;  // This will terminate the main server loop
        System.exit(1);   // Exit the application with a non-zero status
    }
}
