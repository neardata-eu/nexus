package io.nexus.admincli;

import java.util.Scanner;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

public class EntryPoint {
    private static final Jedis redis = new Jedis("localhost", 6379);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static boolean running = true;

    private static PolicyMetadataManager policyCLI;
    private static StreamletDescriptorManager streamletDescriptorCLI;
    private static SwarmletMetadataManager swarmletDescriptorCLI;
    private static StreamletCodeManager streamletCodeManagerCLI;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        policyCLI = new PolicyMetadataManager(scanner, redis, objectMapper);
        streamletDescriptorCLI = new StreamletDescriptorManager(scanner, redis, objectMapper);
        swarmletDescriptorCLI = new SwarmletMetadataManager(scanner, redis, objectMapper);
        streamletCodeManagerCLI = new StreamletCodeManager(scanner, redis);

        while (running) {
            initialPrompt(scanner);
        }

        redis.close();
    }

    public static void initialPrompt(Scanner scanner) {
        System.out.println("\nNexus Metadata Management CLI");
        System.out.println("1. Policy Metadata Management");
        System.out.println("2. Streamlet Metadata Management");
        System.out.println("3. Swarmlet Metadata Management");
        System.out.println("4. Streamlet Code Management");
        System.out.println("5. Exit");

        boolean validChoice = false;
        int answer;

        while (!validChoice & running) {
            validChoice = true;
            answer = scanner.nextInt();
            scanner.nextLine();

            switch (answer) {
                case 1:
                    policyCLI.mainPrompt(scanner);
                    break;
                case 2:
                    streamletDescriptorCLI.mainPrompt();
                    break;
                case 3:
                    swarmletDescriptorCLI.mainPrompt();
                    break;
                case 4:
                    streamletCodeManagerCLI.mainPrompt();
                    break;
                case 5:
                    running = false;
                    break;
                default:
                    validChoice = false;
                    System.out.println("Invalid choice. Try again.");
            }
        }
    }
}
