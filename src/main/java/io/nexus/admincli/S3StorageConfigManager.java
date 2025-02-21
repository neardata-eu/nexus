package io.nexus.admincli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.S3StorageConfig;
import redis.clients.jedis.Jedis;
import java.util.Scanner;

public class S3StorageConfigManager {
    private final Scanner scanner;
    private final Jedis redis;
    private final ObjectMapper objectMapper;

    public S3StorageConfigManager(Scanner scanner, Jedis redis, ObjectMapper objectMapper) {
        this.scanner = scanner;
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public void mainPrompt() {
        System.out.println("\nS3 Storage Configuration Management");
        System.out.println("1. Create S3 Config");
        System.out.println("2. Read S3 Config");
        System.out.println("3. Update S3 Config");
        System.out.println("4. Delete S3 Config");
        System.out.println("5. List All S3 Configs");
        System.out.println("6. Back");

        int choice = scanner.nextInt();
        scanner.nextLine();

        switch (choice) {
            case 1:
                createS3Config();
                break;
            case 2:
                readS3Config();
                break;
            case 3:
                updateS3Config();
                break;
            case 4:
                deleteS3Config();
                break;
            case 5:
                listAllS3Configs();
                break;
            case 6:
                return;
            default:
                System.out.println("Invalid choice. Try again.");
        }
    }

    private void createS3Config() {
        System.out.println("Creating a new S3 Storage Configuration.");
        S3StorageConfig config = new S3StorageConfig();

        System.out.print("Enter id: ");
        config.setId(scanner.nextLine());

        System.out.print("Enter endpoint: ");
        config.setEndpoint(scanner.nextLine());

        System.out.print("Enter access key: ");
        config.setAccessKey(scanner.nextLine());

        System.out.print("Enter secret key: ");
        config.setSecretKey(scanner.nextLine());

        System.out.print("Enter container: ");
        config.setContainer(scanner.nextLine());

        try {
            String configJson = objectMapper.writeValueAsString(config);
            redis.set(MetadataService.METADATA_S3_PREFIX + config.getId(), configJson);
            System.out.println("S3 Storage Configuration saved for endpoint: " + config.getId());
        } catch (JsonProcessingException e) {
            System.out.println("Error saving S3 configuration: " + e.getMessage());
        }
    }

    private void readS3Config() {
        System.out.print("Enter S3 endpoint to read: ");
        String endpoint = MetadataService.METADATA_S3_PREFIX + scanner.nextLine();

        String configJson = redis.get(endpoint);
        if (configJson == null) {
            System.out.println("S3 configuration not found.");
            return;
        }

        try {
            S3StorageConfig config = objectMapper.readValue(configJson, S3StorageConfig.class);
            System.out.println("S3 Storage Configuration Details: ");
            System.out.println(config);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading S3 configuration: " + e.getMessage());
        }
    }

    private void updateS3Config() {
        System.out.print("Enter S3 config id to update: ");
        String id = scanner.nextLine();

        String configJson = redis.get(MetadataService.METADATA_S3_PREFIX + id);
        if (configJson == null) {
            System.out.println("S3 configuration not found.");
            return;
        }

        try {
            S3StorageConfig config = objectMapper.readValue(configJson, S3StorageConfig.class);

            System.out.print("Enter new endpoint (current: " + config.getEndpoint() + "): ");
            config.setEndpoint(scanner.nextLine());

            System.out.print("Enter new access key (current: " + config.getAccessKey() + "): ");
            config.setAccessKey(scanner.nextLine());

            System.out.print("Enter new secret key: ");
            config.setSecretKey(scanner.nextLine());

            System.out.print("Enter new container (current: " + config.getContainer() + "): ");
            config.setContainer(scanner.nextLine());

            redis.set(MetadataService.METADATA_S3_PREFIX + id, objectMapper.writeValueAsString(config));
            System.out.println("S3 configuration updated successfully.");
        } catch (JsonProcessingException e) {
            System.out.println("Error updating S3 configuration: " + e.getMessage());
        }
    }

    private void deleteS3Config() {
        System.out.print("Enter S3 id to delete: ");
        String id = MetadataService.METADATA_S3_PREFIX + scanner.nextLine();

        if (redis.del(id) == 0) {
            System.out.println("S3 configuration not found.");
        } else {
            System.out.println("S3 configuration deleted.");
        }
    }

    private void listAllS3Configs() {
        System.out.println("Listing all S3 Storage Configurations:");

        for (String key : redis.keys(MetadataService.METADATA_S3_PREFIX + "*")) {
            String configJson = redis.get(key);
            try {
                S3StorageConfig config = objectMapper.readValue(configJson, S3StorageConfig.class);
                System.out.println(config);
            } catch (JsonProcessingException e) {
                System.out.println("Error reading S3 configuration: " + e.getMessage());
            }
        }
    }
}
