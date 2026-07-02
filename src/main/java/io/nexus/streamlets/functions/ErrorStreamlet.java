package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;
import org.slf4j.Logger;

/**
 * A Streamlet that intentionally throws an exception for failure testing
 */
public class ErrorStreamlet extends ByteStreamlet {

    private final String name = "ERROR_STREAMLET";

    public ErrorStreamlet() {
    }

    @Override
    public void processPutBytes(StreamletIO event, StreamletContext context) {
        Logger logger = context.getLogger();
        logger.info("ErrorStreamlet " + name + " - THIS STREAMLET IS DESIGNED TO TEST FAILING SCENARIOS");
        throw new RuntimeException("Intentional error in processPutBytes - Testing failure scenario #3");
    }

    @Override
    public void processGetBytes(StreamletIO event, StreamletContext context) {
        Logger logger = context.getLogger();
        logger.info("ErrorStreamlet " + name + " - THIS STREAMLET IS DESIGNED TO TEST FAILING SCENARIOS");
        throw new RuntimeException("Intentional error in processGetBytes - Testing failure scenario #3");
    }
}