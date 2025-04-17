package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.DataSourceStreamlet;
import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.S3StorageConfig;
import io.nexus.streamlets.state.Persistent;
import io.nexus.streamlets.state.StatePersistenceType;
import io.nexus.streamlets.utils.StreamletIO;
import io.pravega.common.io.ByteBufferOutputStream;
import org.slf4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

/**
 * Streamlet that stores data in different storages depending on the output of {@link ImageClassificationStreamlet}.
 */
public class ImageRoutingSharedStreamlet extends ByteStreamlet implements DataSourceStreamlet {
    private final String name;
    @Persistent(type = StatePersistenceType.SHARED)
    private final HashMap<String, S3StorageConfig> persistentMap = new HashMap<>();
    private static final String LOG_FILE_PATH = "/tmp/image-routing-log.txt";

    public ImageRoutingSharedStreamlet() {
        this.name = "IMAGE_ROUTING";
    }

    @Override
    public void processPutBytes(StreamletIO streamletIO, StreamletContext context) {
        Logger logger = context.getLogger();
        Policy policy = context.getPolicy();

        logger.info("PUT - Executing Streamlet: {}, as part of pipeline: {}", name, policy.getPipeline());
        try (InputStream inputStream = streamletIO.input();
             OutputStream pipelineOutput = streamletIO.output();
             ByteBufferOutputStream bufferedData = new ByteBufferOutputStream()) {
            // Buffer data waiting for the previous streamlets to complete.
            doProcess(inputStream, bufferedData, logger);
            // Based on the INFERENCE_KEY, select the storage config.
            S3StorageConfig config = selectStorageConfig(context);
            logger.info("PUT - Streamlet: {}  routing data to {}.", name, config);
            context.routeObjectToPolicyStorage(config, bufferedData.getData().getReader(), bufferedData.size());
            // Log the routing decision to file
            logRoutingToFile(config, logger, bufferedData);
            // Store the location of this chunk.
            this.persistentMap.put(context.getStreamPartition().getScopedObjectName(), config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void logRoutingToFile(S3StorageConfig config, Logger logger, ByteBufferOutputStream bufferedData) {
        String logLine = String.format("%s, %s, %s%n", System.currentTimeMillis(), config.getId(), bufferedData.size());
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true)) {
            writer.write(logLine);
        } catch (IOException e) {
            // If logging fails, silently continue (don't break routing)
            logger.error("Failed to write to routing log file.", e);
        }
    }

    private S3StorageConfig selectStorageConfig(StreamletContext context) {
        String imageClassifierOutput = context.getUserMetadata(ImageClassificationStreamlet.INFERENCE_KEY);
        int humansIdentified = imageClassifierOutput != null ? Integer.parseInt(imageClassifierOutput) : 0;
        context.getLogger().info("Identified {} humans, routing accordingly", humansIdentified);
        // The default (first) configuration is assumed to be for non-human images, whereas the alternative is for human images.
        return humansIdentified == 0 ? context.getS3StorageConfigs().get(0) : context.getS3StorageConfigs().get(1);
    }

    @Override
    public InputStream handlePreGet(StreamPartition streamPartition, StreamletContext context) {
        S3StorageConfig config = this.persistentMap.get(streamPartition.getScopedObjectName());
        context.getLogger().info("PreGET - Streamlet: {} fetching data from {}.", name, config);
        if (config != null) {
            return context.fetchObjectFromPolicyStorage(config, streamPartition);
        }
        return null;
    }

    @Override
    public void processGetBytes(StreamletIO streamletIO, StreamletContext context) {
        throw new UnsupportedOperationException("Streamlet " + name + " is not supposed to implement GET processing.");
    }

    private void doProcess(InputStream input, OutputStream output, Logger logger) {
        int totalBytesRead = 0;
        try {
            int currentBytesRead = 0;
            byte[] target = new byte[8192];
            while ((currentBytesRead = input.read(target)) != -1) {
                output.write(target, 0, currentBytesRead);
                totalBytesRead += currentBytesRead;
            }
            logger.info("Finished Streamlet " + name + " operations. Processed Bytes: " + totalBytesRead);
            output.close();
        } catch (Exception e) {
            logger.error("Error deserializing the input", e);
        }
    }
}
