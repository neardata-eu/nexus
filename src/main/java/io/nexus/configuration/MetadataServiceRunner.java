package io.nexus.configuration;

import java.util.Collections;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import io.nexus.streamlets.metadata.MetadataController;

// Enabling configuration properties to access environment variables to
// construct the configuration dependencies
@SpringBootApplication
@EnableConfigurationProperties({ S3ProxyConfig.class, JCloudsConfig.class, RedisConfig.class })
public class MetadataServiceRunner {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(MetadataController.class);
        // Set the location of the properties file programmatically
        app.setDefaultProperties(
                Collections.singletonMap("spring.config.location", "classpath:/application.properties"));
        app.run(args);
    }
}
