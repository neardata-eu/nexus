package io.nexus.admincli;

import java.util.Scanner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.StreamletDescriptor;
import redis.clients.jedis.Jedis;

public class StreamletDescriptorManagement {
    private Scanner scanner;
    private Jedis redis;
    private ObjectMapper objectMapper;

    public StreamletDescriptorManagement(Scanner scanner, Jedis redis, ObjectMapper objectMapper) {
        this.scanner = scanner;
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public void mainPrompt() {
        System.out.println("\nStreamlets Management");
        System.out.println("1. Create Streamlet");
        System.out.println("2. Read Streamlet");
        System.out.println("3. Update Streamlet");
        System.out.println("4. Delete Streamlet");
        System.out.println("5. List All Streamlets");
        System.out.println("6. Back");

        int choice = scanner.nextInt();
        scanner.nextLine();

        switch (choice) {
            case 1:
                createStreamlet();
                break;
            case 2:
                readStreamlet();
                break;
            case 3:
                updateStreamlet();
                break;
            case 4:
                deleteStreamlet();
                break;
            case 5:
                listAllStreamlets();
                break;
            case 6:
                EntryPoint.initialPrompt(scanner);
                break;
            default:
                System.out.println("Invalid choice. Try again.");
        }
    }

    private void createStreamlet() {
        System.out.println("Creating a new Streamlet.");

        StreamletDescriptor streamletDescriptor = new StreamletDescriptor();
        System.out.print("Enter Streamlet ID: ");
        streamletDescriptor.setId(MetadataService.METADATA_STREAMLET_PREFIX + scanner.nextLine());

        streamletDescriptor.setExecuteOn(inputExecuteOn(scanner));
        streamletDescriptor.setType(inputType(scanner));
        streamletDescriptor.setPartitionLocality(inputPartitionLocality(scanner));
        streamletDescriptor.setResourceUsage(inputResourceUsage(scanner));
        streamletDescriptor.setRequiresGPU(inputRequiresGPU(scanner));

        // Validations and storage
        if (!validateStreamletDescriptor(streamletDescriptor)) {
            System.out.println("Invalid Streamlet data. Please try again.");
            return;
        }

        try {
            String streamletJson = objectMapper.writeValueAsString(streamletDescriptor);
            redis.set(streamletDescriptor.getId(), streamletJson);
            System.out.println("Streamlet created with ID: " +
                    streamletDescriptor.getId());
        } catch (JsonProcessingException e) {
            System.out.println("Error creating Streamlet: " + e.getMessage());
        }

    }

    private void readStreamlet() {
        System.out.print("Enter Streamlet ID to read: ");
        String id = MetadataService.METADATA_STREAMLET_PREFIX + scanner.nextLine();

        String streamletJson = redis.get(id);
        if (streamletJson == null) {
            System.out.println("Streamlet not found.");
            return;
        }

        try {
            StreamletDescriptor streamletDescriptor = objectMapper.readValue(streamletJson, StreamletDescriptor.class);
            System.out.println("Streamlet Details:");
            System.out.println(streamletDescriptor);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading Streamlet: " + e.getMessage());
        }
    }

    private void updateStreamlet() {
        System.out.println("Enter Streamlet ID to update: ");
        String id = MetadataService.METADATA_STREAMLET_PREFIX + scanner.nextLine();

        String streamletDescriptorJson = redis.get(id);
        if (streamletDescriptorJson == null) {
            System.out.println("Streamlet not found.");
            return;
        }

        try {
            StreamletDescriptor streamletDescriptor = objectMapper.readValue(streamletDescriptorJson,
                    StreamletDescriptor.class);

            System.out.println("\nCurrently executes on: " + streamletDescriptor.getExecuteOn());
            streamletDescriptor.setExecuteOn(inputExecuteOn(scanner));

            System.out.println("\nCurrent Streamlet type: " + streamletDescriptor.getType());
            streamletDescriptor.setType(inputType(scanner));

            System.out.println(
                    "\nCurrent partition locality support: "
                            + (streamletDescriptor.isPartitionLocality() ? "Supported" : "Not supported") + " ");
            streamletDescriptor.setPartitionLocality(inputPartitionLocality(scanner));

            System.out.println("\nCurrently resource used: " + streamletDescriptor.getResourceUsage());
            streamletDescriptor.setResourceUsage(inputResourceUsage(scanner));

            System.out.println(
                    "\nCurrently requires GPU: " + (streamletDescriptor.requiresGPU() ? "Yes" : "No"));
            streamletDescriptor.setRequiresGPU(inputRequiresGPU(scanner));
            // Validate Streamlet fields
            if (!validateStreamletDescriptor(streamletDescriptor)) {
                System.out.println("Invalid policy data. Please try again.");
                return;
            }

            redis.set(id, objectMapper.writeValueAsString(streamletDescriptor));
            System.out.println("Streamlet updated successfully.");
        } catch (JsonProcessingException e) {
            System.out.println("Error updating Streamlet: " + e.getMessage());
        }
    }

    private void deleteStreamlet() {
        System.out.print("Enter Streamlet ID to delete: ");
        String streamletId = scanner.nextLine();
        streamletId = MetadataService.METADATA_STREAMLET_PREFIX + streamletId;

        if (redis.del(streamletId) == 0) {
            System.out.println("Streamlet not found.");
        } else {
            System.out.println("Streamlet successfully deleted.");
        }
    }

    private void listAllStreamlets() {
        System.out.println("Listing all Streamlets:");

        for (String key : redis.keys(MetadataService.METADATA_STREAMLET_PREFIX + "*")) {
            String streamletJson = redis.get(key);
            try {
                StreamletDescriptor streamlet = objectMapper.readValue(streamletJson, StreamletDescriptor.class);
                System.out.println(streamlet);
            } catch (JsonProcessingException e) {
                System.out.println("Error reading Streamlet: " + e.getMessage());
            }
        }
    }

    private static boolean validateStreamletDescriptor(StreamletDescriptor streamletDescriptor) {
        if (streamletDescriptor.getId() == null)
            return false;
        if (streamletDescriptor.getExecuteOn() == null)
            return false;
        if (streamletDescriptor.getType() == null)
            return false;
        if (streamletDescriptor.getResourceUsage() == null)
            return false;

        return true;
    }

    private static StreamletDescriptor.ExecuteOn inputExecuteOn(Scanner scanner) {
        System.out.println("Execute on: ");
        System.out.println("1. PUT requests ");
        System.out.println("2. GET requests ");
        System.out.println("3. All Requests ");

        boolean validChoice = false;
        int answer;
        StreamletDescriptor.ExecuteOn input = null;

        while (!validChoice) {
            validChoice = true;
            answer = scanner.nextInt();
            scanner.nextLine();

            switch (answer) {
                case 1:
                    input = StreamletDescriptor.ExecuteOn.PUT;
                    break;
                case 2:
                    input = StreamletDescriptor.ExecuteOn.GET;
                    break;
                case 3:
                    input = StreamletDescriptor.ExecuteOn.ALL;
                    break;
                default:
                    validChoice = false;
                    System.out.println("Invalid choice. Try again.");
            }
        }
        return input;
    }

    private static StreamletDescriptor.Type inputType(Scanner scanner) {
        System.out.println("Enter Streamlet type: ");
        System.out.println("1. Transformer ");
        System.out.println("2. Performance ");
        System.out.println("3. Routing ");
        System.out.println("4. Semantic ");

        boolean validChoice = false;
        int answer;
        StreamletDescriptor.Type input = null;

        while (!validChoice) {
            validChoice = true;
            answer = scanner.nextInt();
            scanner.nextLine();

            switch (answer) {
                case 1:
                    input = StreamletDescriptor.Type.TRANSFORMER;
                    break;
                case 2:
                    input = StreamletDescriptor.Type.PERFORMANCE;
                    break;
                case 3:
                    input = StreamletDescriptor.Type.ROUTING;
                    break;
                case 4:
                    input = StreamletDescriptor.Type.SEMANTIC;
                    break;
                default:
                    validChoice = false;
                    System.out.println("Invalid choice. Try again.");
            }
        }
        return input;
    }

    private static Boolean inputPartitionLocality(Scanner scanner) {
        System.out.println("Benefits from partition locality? (Y/N)");

        String boolResponse = scanner.nextLine();
        boolean validChoice = false;

        while (!validChoice) {
            boolResponse = boolResponse.trim().toUpperCase();
            validChoice = true;
            if (boolResponse.equals("Y")) {
                return true;
            } else if (boolResponse.equals("N")) {
                return false;
            } else {
                System.out.println("Invalid input. Please enter Y or N.");
                validChoice = false;
            }
        }
        return false;
    }

    private static StreamletDescriptor.ResourceUsage inputResourceUsage(Scanner scanner) {
        System.out.println("Enter resource usage: ");
        System.out.println("1. CPU ");
        System.out.println("2. IO ");

        boolean validChoice = false;
        int answer;
        StreamletDescriptor.ResourceUsage input = null;

        while (!validChoice) {
            validChoice = true;
            answer = scanner.nextInt();
            scanner.nextLine();

            switch (answer) {
                case 1:
                    input = StreamletDescriptor.ResourceUsage.CPU;
                    break;
                case 2:
                    input = StreamletDescriptor.ResourceUsage.IO;
                    break;
                default:
                    validChoice = false;
                    System.out.println("Invalid choice. Try again.");
            }
        }
        return input;
    }

    private static boolean inputRequiresGPU(Scanner scanner) {
        System.out.println("Does it require GPU usage? (Y/N)");

        String boolResponse = scanner.nextLine();
        boolean validChoice = false;

        while (!validChoice) {
            boolResponse = boolResponse.trim().toUpperCase();
            validChoice = true;
            if (boolResponse.equals("Y")) {
                return true;
            } else if (boolResponse.equals("N")) {
                return false;
            } else {
                System.out.println("Invalid input. Please enter Y or N.");
                validChoice = false;
            }
        }
        return false;
    }
}
