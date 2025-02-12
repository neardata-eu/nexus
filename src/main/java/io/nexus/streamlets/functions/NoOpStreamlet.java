package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.utils.StreamletIO;

import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;

/**
 * 
 * A basic Streamlet that simply reads the provided data
 * 
 */
public class NoOpStreamlet extends ByteStreamlet {

    private final String name;

    public NoOpStreamlet(String name) {
        this.name = name;
    }

    @Override
    public void processPutBytes(StreamletIO event, StreamletContext context) {
        Logger logger = context.getLogger();
        Policy policy = context.getPolicy();

        // Example of adding metadata
        context.putUserMetadata("encryption", "lz4");

        logger.info("PUT - Executing Streamlet: " + name + ", as part of pipeline: {}", policy.getPipeline());
        doProcess(event.input(), event.output(), logger);
    }

    @Override
    public void processGetBytes(StreamletIO event, StreamletContext context) {
        Logger logger = context.getLogger();
        Policy policy = context.getPolicy();

        // Example of getting metadata
        String encryptionType = context.getUserMetadata("encryption");
        logger.info("User Metadata - Encryption type: {}", encryptionType);

        logger.info("GET - Executing Streamlet: " + name + ", as part of pipeline: {}", policy.getPipeline());
        doProcess(event.input(), event.output(), logger);
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
