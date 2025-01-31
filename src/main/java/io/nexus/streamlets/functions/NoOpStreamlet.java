package io.nexus.streamlets.functions;

import io.nexus.streamlets.StreamletsMetrics;
import io.nexus.streamlets.TransformerStreamlet;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.pravega.common.util.ByteArraySegment;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpStreamlet implements TransformerStreamlet {

    final Logger logger = LoggerFactory.getLogger(NoOpStreamlet.class);
    private final String name;
    private MessageDigest md;

    public NoOpStreamlet(String name) {
        this.name = name;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doPut(ByteBufferPipelineStream input, ByteBufferPipelineStream output) {
        long startTime = System.nanoTime();
        doTransform(input, output);
        StreamletsMetrics.STREAMLET_EXECUTION_TIMER.record(System.nanoTime() - startTime);
    }

    @Override
    public void doGet(ByteBufferPipelineStream input, ByteBufferPipelineStream output) {
        // TODO: placeholder function
        long startTime = System.nanoTime();
        doTransform(input, output);
        StreamletsMetrics.STREAMLET_EXECUTION_TIMER.record(System.nanoTime() - startTime);
    }

    @Override
    public void doTransform(ByteBufferPipelineStream input, ByteBufferPipelineStream output) {
        logger.info(Thread.currentThread() + " -> STREAMLET " + name + " STARTING EXECUTION.");
        int totalBytesRead = 0;

        try {

            int bytesRead = 0;
            while (bytesRead != -1) {
                byte[] target = new byte[8192];
                bytesRead = input.read(target);
                if (bytesRead > 0) {
                    ByteArraySegment readData = new ByteArraySegment(target, 0, bytesRead);
                    output.addSegment(readData);
                    totalBytesRead += bytesRead;
                }
            }
            logger.info("[" + Thread.currentThread() + "-STREAMLET- " + name + "] TOTAL BYTES PROCESSED: " + totalBytesRead);
            output.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
