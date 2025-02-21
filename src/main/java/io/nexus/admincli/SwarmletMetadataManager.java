package io.nexus.admincli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.streamlets.metadata.Hardware;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.SwarmletDescriptor;
import redis.clients.jedis.Jedis;

import java.util.Scanner;

public class SwarmletMetadataManager {

    private Scanner scanner;
    private Jedis redis;
    private ObjectMapper objectMapper;

    public SwarmletMetadataManager(Scanner scanner, Jedis redis, ObjectMapper objectMapper) {
        this.scanner = scanner;
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public void mainPrompt() {
        System.out.println("\nSwarmlet Management");
        System.out.println("1. Create Swarmlet");
        System.out.println("2. Read Swarmlet");
        System.out.println("3. Update Swarmlet");
        System.out.println("4. Delete Swarmlet");
        System.out.println("5. List All Swarmlet");
        System.out.println("6. Back");

        int choice = scanner.nextInt();
        scanner.nextLine();

        switch (choice) {
            case 1:
                createSwarmlet();
                break;
            case 2:
                readSwarmlet();
                break;
            case 3:
                updateSwarmlet();
                break;
            case 4:
                deleteSwarmlet();
                break;
            case 5:
                listAllSwarmlets();
                break;
            case 6:
                EntryPoint.initialPrompt(scanner);
                break;
            default:
                System.out.println("Invalid choice. Try again.");
        }
    }

    private void createSwarmlet() {
        System.out.println("Creating a new Swarmlet.");

        SwarmletDescriptor swarmletDescriptor = new SwarmletDescriptor();
        System.out.print("Enter Swarmlet service endpoint: ");
        swarmletDescriptor.setServiceEndpoint(scanner.nextLine());

        swarmletDescriptor.setRegion(inputRegion(scanner));
        swarmletDescriptor.setHardware(inputHardware(scanner));

        // Validations and storage
        if (!validateSwarmletDescriptor(swarmletDescriptor)) {
            System.out.println("Invalid Swarmlet data. Please try again.");
            return;
        }

        try {
            String streamletJson = objectMapper.writeValueAsString(swarmletDescriptor);
            redis.set(MetadataService.METADATA_SWARMLET_PREFIX + swarmletDescriptor.getServiceEndpoint(), streamletJson);
            System.out.println("Swarmlet created with ID: " + swarmletDescriptor.getServiceEndpoint());
        } catch (JsonProcessingException e) {
            System.out.println("Error creating Swarmlet: " + e.getMessage());
        }
    }

    private static Region inputRegion(Scanner scanner) {
        System.out.println("Enter the region where this Swarmlet is deployed: ");
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

    private static Hardware inputHardware(Scanner scanner) {
        System.out.println("Enter the special hardware for instances in this Swarmlet: ");
        System.out.println("1. NONE ");
        System.out.println("2. GPU ");
        System.out.println("3. TEE ");

        boolean validChoice = false;
        int answer;
        Hardware input = null;

        while (!validChoice) {
            validChoice = true;
            answer = scanner.nextInt();
            scanner.nextLine();

            switch (answer) {
                case 1:
                    input = Hardware.NONE;
                    break;
                case 2:
                    input = Hardware.GPU;
                    break;
                case 3:
                    input = Hardware.TEE;
                    break;
                default:
                    validChoice = false;
                    System.out.println("Invalid choice. Try again.");
            }
        }
        return input;
    }

    private static boolean validateSwarmletDescriptor(SwarmletDescriptor swarmletDescriptor) {
        if (swarmletDescriptor.getServiceEndpoint() == null)
            return false;
        if (swarmletDescriptor.getHardware() == null)
            return false;
        if (swarmletDescriptor.getRegion() == null)
            return false;

        return true;
    }

    private void readSwarmlet() {
        System.out.print("Enter Swarmlet endpoint to read: ");
        String id = MetadataService.METADATA_SWARMLET_PREFIX + scanner.nextLine();

        String swarmletJson = redis.get(id);
        if (swarmletJson == null) {
            System.out.println("Swarmlet not found.");
            return;
        }

        try {
            SwarmletDescriptor swarmletDescriptor = objectMapper.readValue(swarmletJson, SwarmletDescriptor.class);
            System.out.println("Swarmlet Details:");
            System.out.println(swarmletDescriptor);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading Streamlet: " + e.getMessage());
        }
    }

    private void updateSwarmlet() {
        System.out.println("Enter Swarmlet endpoint to update: ");
        String id = scanner.nextLine();

        String swarmletDescriptorJson = redis.get(MetadataService.METADATA_SWARMLET_PREFIX + id);
        if (swarmletDescriptorJson == null) {
            System.out.println("Swarmlet not found.");
            return;
        }

        try {
            SwarmletDescriptor swarmletDescriptor = objectMapper.readValue(swarmletDescriptorJson, SwarmletDescriptor.class);

            System.out.println("\nCurrently in region: " + swarmletDescriptor.getRegion());
            swarmletDescriptor.setRegion(inputRegion(scanner));

            System.out.println("\nCurrent with hardware: " + swarmletDescriptor.getHardware());
            swarmletDescriptor.setHardware(inputHardware(scanner));

            // Validate Streamlet fields
            if (!validateSwarmletDescriptor(swarmletDescriptor)) {
                System.out.println("Invalid Swarmlet data. Please try again.");
                return;
            }

            redis.set(MetadataService.METADATA_SWARMLET_PREFIX + id, objectMapper.writeValueAsString(swarmletDescriptor));
            System.out.println("Swarmlet updated successfully.");
        } catch (JsonProcessingException e) {
            System.out.println("Error updating Swarmlet: " + e.getMessage());
        }
    }

    private void deleteSwarmlet() {
        System.out.print("Enter Swarmlet endpoint to delete: ");
        String swarmletId = scanner.nextLine();
        swarmletId = MetadataService.METADATA_SWARMLET_PREFIX + swarmletId;

        if (redis.del(swarmletId) == 0) {
            System.out.println("Swarmlet not found.");
        } else {
            System.out.println("Swarmlet successfully deleted.");
        }
    }

    private void listAllSwarmlets() {
        System.out.println("Listing all Swarmlets:");

        for (String key : redis.keys(MetadataService.METADATA_SWARMLET_PREFIX + "*")) {
            String swarmletJson = redis.get(key);
            try {
                SwarmletDescriptor swarmlet = objectMapper.readValue(swarmletJson, SwarmletDescriptor.class);
                System.out.println(swarmlet);
            } catch (JsonProcessingException e) {
                System.out.println("Error reading Swarmlet: " + e.getMessage());
            }
        }
    }
}
