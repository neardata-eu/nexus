package io.nexus.admincli;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.Scanner;
import java.util.Set;

import io.nexus.streamlets.metadata.MetadataService;
import redis.clients.jedis.Jedis;

public class StreamletCodeManager {

    private Scanner scanner;
    private Jedis redis;

    public StreamletCodeManager(Scanner scanner, Jedis redis) {
        this.scanner = scanner;
        this.redis = redis;
    }

    public void mainPrompt() {
        System.out.println("\nStreamlet Code Management");
        System.out.println("1. Create Code");
        System.out.println("2. Read Code");
        System.out.println("3. Update Code");
        System.out.println("4. Delete Code");
        System.out.println("5. List All Codes");
        System.out.println("6. Back");

        int choice = scanner.nextInt();
        scanner.nextLine();

        switch (choice) {
            case 1:
                createCode();
                break;
            case 2:
                readCode();
                break;
            case 3:
                updateCode();
                break;
            case 4:
                deleteCode();
                break;
            case 5:
                listAllCodes();
                break;
            case 6:
                EntryPoint.initialPrompt(scanner);
                break;
            default:
                System.out.println("Invalid choice. Try again.");
        }
    }

    private void createCode() {
        System.out.print("Enter Streamlet/Deserializer name: ");
        String name = scanner.nextLine();
        System.out.print("Enter the path to the code file: ");
        String filePath = scanner.nextLine();
        try {
            String code = new String(Files.readAllBytes(Paths.get(filePath)));
            redis.set(MetadataService.METADATA_STREAMLET_CODE_PREFIX + name, code);
            System.out.println("Code saved successfully.");
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
    }

    private void readCode() {
        System.out.print("Enter Streamlet/Deserializer name: ");
        String name = scanner.nextLine();
        String code = redis.get(MetadataService.METADATA_STREAMLET_CODE_PREFIX + name);
        if (code != null) {
            System.out.println("Code:");
            System.out.println(code);
        } else {
            System.out.println("No code found for: " + name);
        }
    }

    private void updateCode() {
        System.out.print("Enter Streamlet/Deserializer name: ");
        String name = scanner.nextLine();
        System.out.print("Enter the path to the updated code file: ");
        String filePath = scanner.nextLine();
        try {
            String code = new String(Files.readAllBytes(Paths.get(filePath)));
            redis.set(MetadataService.METADATA_STREAMLET_CODE_PREFIX + name, code);
            System.out.println("Code updated successfully.");
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
    }

    private void deleteCode() {
        System.out.print("Enter Streamlet/Deserializer id (i.e., fully qualified class name): ");
        String name = scanner.nextLine();
        redis.del(MetadataService.METADATA_STREAMLET_CODE_PREFIX + name);
        System.out.println("Code deleted successfully.");
    }

    private void listAllCodes() {
        Set<String> keys = redis.keys(MetadataService.METADATA_STREAMLET_CODE_PREFIX + "*");
        if (keys.isEmpty()) {
            System.out.println("No stored Streamlet/Deserializer codes.");
            return;
        }
        System.out.println("Stored Streamlet/Deserializer codes:");
        for (String key : keys) {
            System.out.println(key.replace(MetadataService.METADATA_STREAMLET_CODE_PREFIX, ""));
        }
    }
}
