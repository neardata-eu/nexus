package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;
import org.slf4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * 
 * A basic Streamlet that simply reads the provided data
 * 
 */
public class NoOpStreamlet2 extends ByteStreamlet {

    private final String name = "NOOP2";

    public NoOpStreamlet2() {}

    @Override
    public void processPutBytes(StreamletIO event, StreamletContext context) {
        Logger logger = context.getLogger();
        doProcess(event.input(), event.output(), logger);
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
            logger.info("Finished Streamlet " + name + " operations. Processed Bytes: " + totalBytesRead);
            output.close();
        } catch (Exception e) {
            logger.error("Error deserializing the input", e);
        }
    }
}
