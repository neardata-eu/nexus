package io.nexus.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

/**
 * Class to encapsulate metrics initialization
 */

public class ServerConfig {
    private static final Logger logger = LoggerFactory.getLogger(ServerConfig.class);
    private final static String PROPERTY_NAME = "webserver";

    public ServerConfig(PropertiesLoader config) {
        try {
            JvmMetrics.builder().register();
            HTTPServer server = HTTPServer.builder()
                    .port(config.getInt(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "port")).buildAndStart();

            logger.info("Initialized metrics webserver listening on port " + server.getPort() + "/metrics");

        } catch (Exception e) {
            logger.error("Unable to initialize metrics http server", e);
        }
    }
}
