package io.nexus.streamlets;

import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;
import org.slf4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;

import static io.nexus.streamlets.StreamletsMetrics.GET_STREAMLET_EXECUTION_LATENCY_TIMER;
import static io.nexus.streamlets.StreamletsMetrics.PUT_STREAMLET_EXECUTION_LATENCY_TIMER;

/**
 * A Streamlet that processes raw byte streams. Implementing classes must define how bytes are processed.
 * Important: due to the dynamic loading capabilities of Streamlets, Nexus assumes that Streamlets extending this
 * class are instantiated via a constructor with no arguments.
 */
public abstract class ByteStreamlet implements Streamlet {

    /**
     * Processes raw byte streams in PUT requests.
     *
     * @param dataStreams   The input and output data streams to be processed during the PUT operation.
     * @param context       The context in which the PUT operation is being handled, providing the user with read access
     *                      to the context information, alongside read/write access to the blob's user metadata.
     */
    protected abstract void processPutBytes(StreamletIO dataStreams, StreamletContext context);

    /**
     * Processes raw byte streams in GET requests.
     *
     * @param dataStreams   The input and output data streams to be processed during the GET operation.
     * @param context       The context in which the GET operation is being handled, providing the user with read access
     *                      to the context information, alongside read/write access to the blob's user metadata.
     */
    protected abstract void processGetBytes(StreamletIO dataStreams, StreamletContext context);

    @Override
    public final void handlePut(StreamletIO dataStreams, StreamletContext context) {
        long startTime = System.nanoTime();
        processPutBytes(dataStreams, context);
        PUT_STREAMLET_EXECUTION_LATENCY_TIMER.record(System.nanoTime() - startTime);
    }

    @Override
    public final void handleGet(StreamletIO dataStreams, StreamletContext context) {
        long startTime = System.nanoTime();
        processGetBytes(dataStreams, context);
        GET_STREAMLET_EXECUTION_LATENCY_TIMER.record(System.nanoTime() - startTime);
    }

    protected int doProcess(InputStream input, OutputStream output, String streamletName, Logger logger) {
        int totalBytesRead = 0;
        try {
            int currentBytesRead = 0;
            byte[] target = new byte[8192];
            while ((currentBytesRead = input.read(target)) != -1) {
                output.write(target, 0, currentBytesRead);
                totalBytesRead += currentBytesRead;
            }
            logger.info("Finished Streamlet " + streamletName + " operations. Processed Bytes: " + totalBytesRead);
            output.close();
            return totalBytesRead;
        } catch (Exception e) {
            logger.error("Error deserializing the input", e);
            throw new RuntimeException(e);
        }
    }
}
