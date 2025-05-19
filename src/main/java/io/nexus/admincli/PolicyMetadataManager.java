package io.nexus.admincli;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import io.nexus.streamlets.metadata.*;
import redis.clients.jedis.Jedis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PolicyMetadataManager {
    private Scanner scanner;
    private Jedis redis;
    private ObjectMapper objectMapper;

    public PolicyMetadataManager(Scanner scanner, Jedis redis, ObjectMapper objectMapper) {
        this.scanner = scanner;
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public void mainPrompt(Scanner scanner) {
        System.out.println("\nPolicy Management");
        System.out.println("1. Create Policy");
        System.out.println("2. Read Policy");
        System.out.println("3. Update Policy");
        System.out.println("4. Delete Policy");
        System.out.println("5. List All Policies");
        System.out.println("6. Back");

        int choice = scanner.nextInt();
        scanner.nextLine();

        switch (choice) {
            case 1:
                createPolicy();
                break;
            case 2:
                readPolicy();
                break;
            case 3:
                updatePolicy();
                break;
            case 4:
                deletePolicy();
                break;
            case 5:
                listAllPolicies();
                break;
            case 6:
                EntryPoint.initialPrompt(scanner);
                break;
            default:
                System.out.println("Invalid choice. Try again.");
        }
    }

    private void createPolicy() {
        System.out.println("Creating a new Policy.");

        Policy policy = new Policy();
        System.out.print("Enter policy id: ");
        policy.setId(scanner.nextLine());

        System.out.print("Enter system: ");
        policy.setSystem(scanner.nextLine());

        System.out.print("Enter scope: ");
        policy.setScope(scanner.nextLine());

        System.out.print("Enter stream: ");
        policy.setStream(scanner.nextLine());

        policy.setPipeline(inputPipeline(scanner));
        policy.setStorage(inputList(scanner, "storage"));

        // Validate Policy fields
        if (!validatePolicy(policy)) {
            System.out.println("Invalid policy data. Please try again.");
            return;
        }

        // Store in Redis
        try {
            String policyJson = objectMapper.writeValueAsString(policy);
            redis.set(MetadataService.METADATA_POLICY_PREFIX + policy.getId(), policyJson);
            System.out.println("Policy created with ID: " + policy.getId());
        } catch (JsonProcessingException | IncorrectMetadataException e) {
            System.out.println("Error creating policy: " + e.getMessage());
        }
    }

    private void readPolicy() {
        System.out.print("Enter Policy ID to read: ");
        String id = MetadataService.METADATA_POLICY_PREFIX + scanner.nextLine();

        String policyJson = redis.get(id);
        if (policyJson == null) {
            System.out.println("Policy not found.");
            return;
        }

        try {
            Policy policy = objectMapper.readValue(policyJson, Policy.class);
            System.out.println("Policy Details: ");
            System.out.println(policy);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading policy: " + e.getMessage());
        }
    }

    private void updatePolicy() {
        System.out.print("Enter Policy ID to update: ");
        String id = scanner.nextLine();

        String policyJson = redis.get(MetadataService.METADATA_POLICY_PREFIX + id);
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

            policy.setPipeline(inputPipeline(scanner));
            policy.setStorage(inputList(scanner, "storage"));

            // Validate Policy fields
            if (!validatePolicy(policy)) {
                System.out.println("Invalid policy data. Please try again.");
                return;
            }

            // Update in Redis
            redis.set(MetadataService.METADATA_POLICY_PREFIX + id, objectMapper.writeValueAsString(policy));
            System.out.println("Policy updated successfully.");
        } catch (JsonProcessingException | IncorrectMetadataException e) {
            System.out.println("Error updating policy: " + e.getMessage());
        }
    }

    private void deletePolicy() {
        System.out.print("Enter Policy ID to delete: ");
        String id = scanner.nextLine();
        id = MetadataService.METADATA_POLICY_PREFIX + id;

        if (redis.del(id) == 0) {
            System.out.println("Policy not found.");
        } else {
            System.out.println("Policy deleted.");
        }
    }

    private void listAllPolicies() {
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
        if (policy.getSystem() == null || policy.getSystem().isEmpty())
            return false;
        if (policy.getScope() == null || policy.getScope().isEmpty())
            return false;
        if (policy.getStream() == null || policy.getStream().isEmpty())
            return false;
        return policy.getPipeline() != null && !policy.getPipeline().isEmpty();
    }

    private List<String> inputList(Scanner scanner, String fieldName) {
        System.out.print("Enter " + fieldName + " values (comma-separated): ");
        String input = scanner.nextLine().trim();

        List<String> list = new ArrayList<>();
        if (!input.isEmpty()) {
            String[] values = input.split(",");
            for (String value : values) {
                String trimmed = value.trim();
                if (!trimmed.isEmpty()) {
                    list.add(trimmed);
                }
            }
        }
        return list;
    }

    private List<StreamletExecutionDescriptor> inputPipeline(Scanner scanner) {
        List<StreamletExecutionDescriptor> streamletPipeline = new ArrayList<>();
        String userInput;
        do {
            System.out.print("Enter Streamlet ID to read (it should exist in the system): ");
            String id = scanner.nextLine();
            StreamletDescriptor streamlet = getStreamletDescriptor(id);
            if (streamlet == null) {
                System.out.println("The Streamlets of a Policy should be installed first.");
                throw new IncorrectMetadataException("Trying to configure a Policy with non-existent Streamlets.");
            }
            Region region = inputRegion(scanner);
            List<String> args = inputList(scanner, "streamletArgs");
            streamletPipeline.add(new StreamletExecutionDescriptor(streamlet, region, args));
            System.out.println("Do you want to continue adding Streamlets? (yes/no): ");
            userInput = scanner.nextLine();
        } while (userInput.equalsIgnoreCase("yes"));
        return streamletPipeline;
    }
    
    private StreamletDescriptor getStreamletDescriptor(String id) {
        String streamletJson = redis.get(MetadataService.METADATA_STREAMLET_PREFIX + id);
        StreamletDescriptor streamlet = null;
        if (streamletJson == null) {
            System.out.println("Streamlet not found.");
            return streamlet;
        }

        try {
            streamlet = objectMapper.readValue(streamletJson, StreamletDescriptor.class);
            System.out.println("Streamlet Details:");
            System.out.println(streamlet);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading Streamlet: " + e.getMessage());
        }
        return streamlet;
    }

    private static Region inputRegion(Scanner scanner) {
        System.out.println("Enter the region to execute this Streamlet: ");
        System.out.println("1. EDGE ");
        System.out.println("2. CLOUD ");

        boolean validChoice = false;
        int answer;
        Region input = null;

        while (!validChoice) {
            validChoice = true;
            answer = scanner.nextInt();
            scanner.nextLine();

            switch (answer) {
                case 1:
                    input = Region.EDGE;
                    break;
                case 2:
                    input = Region.CLOUD;
                    break;
                default:
                    validChoice = false;
                    System.out.println("Invalid choice. Try again.");
            }
        }
        return input;
    }
}
