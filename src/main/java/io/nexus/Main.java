package io.nexus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import io.nexus.configuration.MetadataServiceRunner;

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

        while (running) {
            logger.info("Main server task is running...");
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
        running = false; // This will terminate the main server loop
        System.exit(1); // Exit the application with a non-zero status
    }
}
