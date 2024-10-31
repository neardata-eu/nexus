package io.nexus.admincli;

import java.util.Scanner;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;

public class EntryPoint {
    private static final Jedis redis = new Jedis("localhost", 6379);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static boolean running = true;

    private static PolicyManagment policyCLI;
    private static StreamletDescriptorManagement streamletDescriptorCLI;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        policyCLI = new PolicyManagment(scanner, redis, objectMapper);
        streamletDescriptorCLI = new StreamletDescriptorManagement(scanner, redis, objectMapper);

        while (running) {
            initialPrompt(scanner);
        }

        redis.close();
    }

    public static void initialPrompt(Scanner scanner) {
        System.out.println("\nNexus Management CLI");
        System.out.println("1. Policy Management");
        System.out.println("2. Streamlet Management");
        System.out.println("3. Exit");

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
                    running = false;
                    break;
                default:
                    validChoice = false;
                    System.out.println("Invalid choice. Try again.");
            }
        }

    }
}
