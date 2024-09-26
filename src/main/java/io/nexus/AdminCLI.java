package io.nexus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Policy;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class AdminCLI {

    private static final Jedis redis = new Jedis("localhost", 6379);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            System.out.println("\nPolicy Management CLI");
            System.out.println("1. Create Policy");
            System.out.println("2. Read Policy");
            System.out.println("3. Update Policy");
            System.out.println("4. Delete Policy");
            System.out.println("5. List All Policies");
            System.out.println("6. Exit");

            int choice = scanner.nextInt();
            scanner.nextLine(); // consume newline

            switch (choice) {
                case 1:
                    createPolicy(scanner);
                    break;
                case 2:
                    readPolicy(scanner);
                    break;
                case 3:
                    updatePolicy(scanner);
                    break;
                case 4:
                    deletePolicy(scanner);
                    break;
                case 5:
                    listAllPolicies();
                    break;
                case 6:
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice. Try again.");
            }
        }

        redis.close();
    }

    private static void createPolicy(Scanner scanner) {
        System.out.println("Creating a new Policy.");

        Policy policy = new Policy();
        System.out.print("Enter policy id: ");
        policy.setId(MetadataService.METADATA_POLICY_PREFIX + scanner.nextLine());

        System.out.print("Enter system: ");
        policy.setSystem(scanner.nextLine());

        System.out.print("Enter scope: ");
        policy.setScope(scanner.nextLine());

        System.out.print("Enter stream: ");
        policy.setStream(scanner.nextLine());

        policy.setPipeline(inputList(scanner, "pipeline"));
        policy.setStorage(inputList(scanner, "storage"));

        // Validate Policy fields
        if (!validatePolicy(policy)) {
            System.out.println("Invalid policy data. Please try again.");
            return;
        }

        // Store in Redis
        try {
            String policyJson = objectMapper.writeValueAsString(policy);
            redis.set(policy.getId(), policyJson);
            System.out.println("Policy created with ID: " + policy.getId());
        } catch (JsonProcessingException e) {
            System.out.println("Error creating policy: " + e.getMessage());
        }
    }

    private static void readPolicy(Scanner scanner) {
        System.out.print("Enter Policy ID to read: ");
        String id = MetadataService.METADATA_POLICY_PREFIX + scanner.nextLine();

        String policyJson = redis.get(id);
        if (policyJson == null) {
            System.out.println("Policy not found.");
            return;
        }

        try {
            Policy policy = objectMapper.readValue(policyJson, Policy.class);
            System.out.println("Policy Details:");
            System.out.println(policy);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading policy: " + e.getMessage());
        }
    }

    private static void updatePolicy(Scanner scanner) {
        System.out.print("Enter Policy ID to update: ");
        String id = MetadataService.METADATA_POLICY_PREFIX + scanner.nextLine();

        String policyJson = redis.get(id);
        if (policyJson == null) {
            System.out.println("Policy not found.");
            return;
        }

        try {
            Policy policy = objectMapper.readValue(policyJson, Policy.class);

            System.out.print("Enter new system (current: " + policy.getSystem() + "): ");
            policy.setSystem(scanner.nextLine());

            System.out.print("Enter new scope (current: " + policy.getScope() + "): ");
            policy.setScope(scanner.nextLine());

            System.out.print("Enter new stream (current: " + policy.getStream() + "): ");
            policy.setStream(scanner.nextLine());

            policy.setPipeline(inputList(scanner, "pipeline"));
            policy.setStorage(inputList(scanner, "storage"));

            // Validate Policy fields
            if (!validatePolicy(policy)) {
                System.out.println("Invalid policy data. Please try again.");
                return;
            }

            // Update in Redis
            redis.set(id, objectMapper.writeValueAsString(policy));
            System.out.println("Policy updated successfully.");
        } catch (JsonProcessingException e) {
            System.out.println("Error updating policy: " + e.getMessage());
        }
    }

    private static void deletePolicy(Scanner scanner) {
        System.out.print("Enter Policy ID to delete: ");
        String id = scanner.nextLine();
        id = MetadataService.METADATA_POLICY_PREFIX + id;

        if (redis.del(id) == 0) {
            System.out.println("Policy not found.");
        } else {
            System.out.println("Policy deleted.");
        }
    }

    private static void listAllPolicies() {
        System.out.println("Listing all policies:");

        for (String key : redis.keys(MetadataService.METADATA_POLICY_PREFIX + "*")) {
            String policyJson = redis.get(key);
            try {
                Policy policy = objectMapper.readValue(policyJson, Policy.class);
                System.out.println(policy);
            } catch (JsonProcessingException e) {
                System.out.println("Error reading policy: " + e.getMessage());
            }
        }
    }

    private static boolean validatePolicy(Policy policy) {
        if (policy.getSystem() == null || policy.getSystem().isEmpty()) return false;
        if (policy.getScope() == null || policy.getScope().isEmpty()) return false;
        if (policy.getStream() == null || policy.getStream().isEmpty()) return false;
        if (policy.getPipeline() == null || policy.getPipeline().isEmpty()) return false;
        if (policy.getStorage() == null || policy.getStorage().isEmpty()) return false;

        return true;
    }

    private static List<String> inputList(Scanner scanner, String fieldName) {
        System.out.print("Enter " + fieldName + " values (comma-separated): ");
        String input = scanner.nextLine();
        String[] values = input.split(",");
        List<String> list = new ArrayList<>();
        for (String value : values) {
            list.add(value.trim());
        }
        return list;
    }
}

