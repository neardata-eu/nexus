package io.nexus.streamlets.metadata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;

@SpringBootApplication
public class MetadataServiceRunner {

    public static void main(String[] args) {
        System.err.println("HOOOOOOOOOOOOOOOOOOOLAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        SpringApplication app = new SpringApplication(MetadataController.class);
        // Set the location of the properties file programmatically
        app.setDefaultProperties(Collections.singletonMap("spring.config.location", "classpath:/application.properties"));
        app.run(args);
    }
}
