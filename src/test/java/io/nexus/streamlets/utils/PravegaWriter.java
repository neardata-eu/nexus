package io.nexus.streamlets.utils;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class PravegaWriter {

    private static final String SCOPE = "scope-pravega-buffering";
    private static final String STREAM_NAME = "example-stream";
    private static final URI CONTROLLER_URI = URI.create("tcp://localhost:9090"); // Adjust as needed

    public static void main(String[] args) throws InterruptedException {
        // Configurable parameters
        int eventsPerSecond = 100;     // Number of events per second
        int eventSizeBytes = 10240;     // Size of each event in bytes
        int durationSeconds = 300;     // Duration to run the writer

        // Create scope and stream
        StreamManager streamManager = StreamManager.create(CONTROLLER_URI);
        streamManager.createScope(SCOPE);
        streamManager.createStream(SCOPE, STREAM_NAME, StreamConfiguration.builder().build());

        // Create client factory and writer
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(CONTROLLER_URI).build();
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(
                     STREAM_NAME,
                     new JavaSerializer<>(),
                     EventWriterConfig.builder().build())) {

            long endTime = System.currentTimeMillis() + durationSeconds * 1000;
            Random random = new Random();

            while (System.currentTimeMillis() < endTime) {
                long start = System.currentTimeMillis();

                for (int i = 0; i < eventsPerSecond; i++) {
                    String event = generateRandomString(eventSizeBytes, random);
                    writer.writeEvent(event);
                    System.out.println("Written event of size " + event.length() + " bytes");
                }

                long elapsed = System.currentTimeMillis() - start;
                long sleepTime = 1000 - elapsed;
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
            }
        }

        System.out.println("Finished writing events to Pravega.");
    }

    private static String generateRandomString(int sizeBytes, Random random) {
        byte[] bytes = new byte[sizeBytes];
        random.nextBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
