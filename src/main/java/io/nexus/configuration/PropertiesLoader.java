package io.nexus.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Properly reading properties through environment variables first
 * If there are no environment variables, use the already-loaded application.properties
 * Throw an exception otherwise
 */
public class PropertiesLoader {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);

    private final Properties properties = new Properties();
    public final static char SEPARATOR = '.';

    public PropertiesLoader() {
        loadProperties();
    }

    private void loadProperties() {
        // Check upon boot up if application.properties is there
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                logger.error("Couldn't find application.properties file");
                throw new RuntimeException();
            }
            properties.load(input);
        } catch (IOException e) {
            logger.error("Error while loading configuration");
            throw new RuntimeException(e);
        }
    }

    public String getString(String key) {
        String envValue = getEnvironmentVariable(key);
        return envValue != null ? envValue : properties.getProperty(key);
    }

    public int getInt(String key) {
        String envValue = getEnvironmentVariable(key);
        return envValue != null ? Integer.parseInt(envValue) : Integer.parseInt(properties.getProperty(key));
    }

    private static String getEnvironmentVariable(String key) {
        return System.getenv(key.toUpperCase().replace(SEPARATOR, '_'));

    }
}