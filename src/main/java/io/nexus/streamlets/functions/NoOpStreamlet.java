package io.nexus.streamlets.functions;

import io.nexus.streamlets.Streamlet;
import io.nexus.streamlets.utils.DynamicInputStream;
import io.pravega.common.io.ByteBufferOutputStream;

import java.io.IOException;
import java.io.InputStream;

public class NoOpStreamlet implements Streamlet {

    @Override
    public InputStream processPut(DynamicInputStream input) {
        System.err.println(Thread.currentThread() + " -> STREAMLET RUNNING FUTURE ");
        int totalBytesRead = 0;
        byte[] target = new byte[1];
        ByteBufferOutputStream output = new ByteBufferOutputStream();
        try {
            int bytesRead = 0;
            while (bytesRead >= 0) {
                bytesRead = input.read(target, 0, target.length);
                System.err.println("STREAMLET -> WRITING BYTES FROM INPUT STREAM FOR LOGGING " + bytesRead);
                output.write(target);
                output.write(target);
                totalBytesRead += bytesRead;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        output.close();
        return output.getData().getReader();
    }
}
