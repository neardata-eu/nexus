package io.nexus.streamlets.utils;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.ScalingPolicy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class FastqPravegaByteProducer {

    public static void main(String[] args) throws IOException {
        // Config
        String scope = "examples";
        String streamName = "fastqstream";
        URI controllerUri = URI.create("tcp://localhost:9090"); // adjust if needed

        // Setup Pravega Stream
        try (StreamManager streamManager = StreamManager.create(ClientConfig.builder()
                .controllerURI(controllerUri)
                .build())) {

            streamManager.createScope(scope);
            streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build());
        }

        // Read the FASTQ file and write to Pravega
        try (InputStream inputStream = FastqPravegaByteProducer.class.getClassLoader().getResourceAsStream("sample.fastq");
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
             ByteStreamClientFactory clientFactory = ByteStreamClientFactory.withScope(scope,
                        ClientConfig.builder().controllerURI(controllerUri).build());
             ByteStreamWriter writer = clientFactory.createByteStreamWriter(streamName)) {
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write((line + "\n").getBytes(StandardCharsets.UTF_8));
            }
            writer.flush();
            System.out.println("FASTQ data written to stream: " + streamName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
