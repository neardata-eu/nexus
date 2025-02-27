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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Random;
/**
 * 
 * Streamlet that stores data in different storages depending on some random decision.
 * 
 */
public class SimpleRoutingStreamlet extends ByteStreamlet implements DataSourceStreamlet {

    private final String name;
    @Persistent(name = "dataRoutingMap", type = StatePersistenceType.SHARED)
    private final HashMap<String, S3StorageConfig> persistentMap = new HashMap<>();

    public SimpleRoutingStreamlet() {
        this.name = "SIMPLE_ROUTING";
    }

    @Override
    public void processPutBytes(StreamletIO streamletIO, StreamletContext context) {
        Logger logger = context.getLogger();
        Policy policy = context.getPolicy();

        logger.info("PUT - Executing Streamlet: {}, as part of pipeline: {}", name, policy.getPipeline());
        try (InputStream inputStream = streamletIO.input();
             OutputStream pipelineOutput = streamletIO.output();
             ByteBufferOutputStream bufferedData = new ByteBufferOutputStream()) {
            doProcess(inputStream, bufferedData, logger);
            S3StorageConfig config = context.getS3StorageConfigs().get(new Random().nextInt(0 ,context.getS3StorageConfigs().size()));
            logger.info("PUT - Streamlet: {}  routing data to {}.", name, config);
            context.routeObjectToPolicyStorage(config, bufferedData.getData().getReader(), bufferedData.size());
            this.persistentMap.put(context.getStreamPartition().getScopedObjectName(), config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
