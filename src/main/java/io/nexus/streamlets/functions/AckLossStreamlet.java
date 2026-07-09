// src/main/java/io/nexus/streamlets/functions/AckLossStreamlet.java
package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;

import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;

/**
 * A Streamlet that intentionally throws an exception after processing and
 * completing storage operations
 */

public class AckLossStreamlet extends ByteStreamlet {

    @Override
    public void processPutBytes(StreamletIO event, StreamletContext context) {
        Logger logger = context.getLogger();

        try {
            doProcess(event.input(), event.output(), logger);
            throw new RuntimeException("ACK LOSS AFTER PROCESSING");

        } catch (Exception e) {
            logger.error("Ack loss simulation: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processGetBytes(StreamletIO event, StreamletContext context) {
        Logger logger = context.getLogger();
        doProcess(event.input(), event.output(), logger);
    }

    private void doProcess(InputStream input, OutputStream output, Logger logger) {
        int totalBytesRead = 0;
        try {
            int currentBytesRead = 0;
            byte[] target = new byte[64 * 1024];
            while ((currentBytesRead = input.read(target)) != -1) {
                output.write(target, 0, currentBytesRead);
                totalBytesRead += currentBytesRead;
            }
            logger.info("Finished Streamlet " + "ACK LOSS" + " operations. Processed Bytes: " + totalBytesRead);
            output.close();
        } catch (Exception e) {
            logger.error("Error deserializing the input", e);
        }
    }
}
