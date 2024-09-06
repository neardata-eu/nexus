package io.nexus.streamlets.functions;

import io.nexus.streamlets.TransformerStreamlet;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.pravega.common.util.ByteArraySegment;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class NoOpStreamlet implements TransformerStreamlet {

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
    public void doTransform(ByteBufferPipelineStream input, ByteBufferPipelineStream output) {
        System.err.println(Thread.currentThread() + " -> STREAMLET " + name + " RUNNING FUTURE ");
        int totalBytesRead = 0;

        int iniByte = 0;
        int endByte = 0;
        int iteration = 0;
        try {
            int bytesRead = 0;
            while (bytesRead != -1) {
                byte[] target = new byte[8192];
                bytesRead = input.read(target);
                if (bytesRead > 0) {
                    ByteArraySegment readData = new ByteArraySegment(target, 0, bytesRead);
                    output.addSegment(readData);
                    totalBytesRead += bytesRead;
                    endByte += bytesRead;
                    iteration++;
                }
            }
            System.err.println("[" + Thread.currentThread() + "-STREAMLET- " + name +
                    "] TOTAL BYTES PROCESSED" + totalBytesRead);
            output.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
