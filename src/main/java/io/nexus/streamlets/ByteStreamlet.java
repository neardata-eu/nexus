package io.nexus.streamlets;

import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;

import static io.nexus.streamlets.StreamletsMetrics.GET_STREAMLET_EXECUTION_LATENCY_TIMER;
import static io.nexus.streamlets.StreamletsMetrics.PUT_STREAMLET_EXECUTION_LATENCY_TIMER;

/**
 * A Streamlet that processes raw byte streams. Implementing classes must define how bytes are processed.
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
}
