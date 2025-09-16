package io.nexus.streamlets.functions;

import io.nexus.streamlets.EventStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.deserializers.PravegaImageDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PravegaJPGEventStreamlet extends EventStreamlet<byte[]> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String name = "PRAVEGA_JPG_STREAMLET";

    public PravegaJPGEventStreamlet(PravegaImageDeserializer deserializer) {
        super(deserializer);
    }

    @Override
    protected void processPutRecord(byte[] imageBytes, StreamletContext context) {
        if (isValidJpg(imageBytes)) {
                logger.info(name + ": Valid JPEG image received: {} bytes", imageBytes.length);

            } else {
                logger.warn(name + ": Invalid or non-JPEG image detected: {} bytes, data: {}", imageBytes.length, imageBytes);
        }
    }

    @Override
    protected void processGetRecord(byte[] record, StreamletContext context) {
        throw new UnsupportedOperationException("GET not supported.");
    }

    private boolean isValidJpg(byte[] imageData) {
        if (imageData == null || imageData.length < 2) {
            return false;
        }
        
        return (imageData[0] & 0xFF) == 0xFF && 
               (imageData[1] & 0xFF) == 0xD8;
    }
}

