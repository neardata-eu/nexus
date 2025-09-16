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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public class PersonalInformationRoutingStreamlet extends ByteStreamlet implements DataSourceStreamlet {
    private static final Pattern EMAIL_PATTERN = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
    private static final Pattern PHONE_PATTERN = Pattern.compile("\\b(?:\\+\\d{1,3}\\s?)?(?:\\(\\d{1,4}\\)|\\d{1,4})[-\\s]?\\d{1,4}[-\\s]?\\d{3,4}\\b");
    private static final Pattern CREDIT_CARD_PATTERN = Pattern.compile("\\b(?:\\d[ -]*?){13,16}\\b");

    private final String name;

    @Persistent(type = StatePersistenceType.SHARED)
    private final HashMap<String, S3StorageConfig> persistentMap = new HashMap<>();

    public PersonalInformationRoutingStreamlet() {
        this.name = "PERSONAL_INFORMATION_ROUTING";
    }

    @Override
    public void processPutBytes(StreamletIO streamletIO, StreamletContext context) {
        Logger logger = context.getLogger();
        Policy policy = context.getPolicy();

        logger.info("PUT - Executing Streamlet: {}, as part of pipeline: {}", name, policy.getPipeline());

        try (InputStream inputStream = streamletIO.input();
             OutputStream pipelineOutput = streamletIO.output();
             ByteBufferOutputStream bufferedData = new ByteBufferOutputStream()) {

            // NEW: Do inline PII scan while streaming bytes
            boolean containsPII = doProcess(inputStream, bufferedData, logger);

            // Choose storage based on PII flag
            S3StorageConfig targetConfig = chooseStorageTarget(context, containsPII);
            logger.info("PUT - Streamlet: {} routing data to {}, PII detected: {}", name, targetConfig, containsPII);

            context.routeObjectToPolicyStorage(targetConfig, bufferedData.getData().getReader(), bufferedData.size());
            this.persistentMap.put(context.getStreamPartition().getScopedPartitionUri(), targetConfig);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Streams bytes from input to output while checking for PII in each chunk.
     */
    private boolean doProcess(InputStream input, OutputStream output, Logger logger) {
        int totalBytesRead = 0;
        byte[] buffer = new byte[8192];
        int bytesRead;
        boolean piiDetected = false;

        try {
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;

                if (!piiDetected) {
                    String chunk = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
                    piiDetected = containsPII(chunk);
                }
            }

            output.flush();
            logger.info("Finished Streamlet {} operations. Processed Bytes: {}", name, totalBytesRead);
        } catch (IOException e) {
            logger.error("Error processing input bytes in Streamlet: " + name, e);
        }

        return piiDetected;
    }

    private boolean containsPII(String text) {
        return EMAIL_PATTERN.matcher(text).find()
                || PHONE_PATTERN.matcher(text).find()
                || CREDIT_CARD_PATTERN.matcher(text).find();
    }

    private S3StorageConfig chooseStorageTarget(StreamletContext context, boolean containsPII) {
        List<S3StorageConfig> configs = context.getS3StorageConfigs();
        if (containsPII) {
            return configs.get(0); // Assume first config for sensitive
        } else {
            return configs.get(configs.size() > 1 ? 1 : 0); // Others for non-sensitive
        }
    }

    @Override
    public InputStream handlePreGet(StreamPartition streamPartition, StreamletContext context) {
        S3StorageConfig config = this.persistentMap.get(streamPartition.getScopedPartitionUri());
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
}
