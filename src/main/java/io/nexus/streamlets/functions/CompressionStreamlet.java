package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionStreamlet extends ByteStreamlet {

    private final String name = "COMPRESSION";

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        try (InputStream input = dataStreams.input();
             OutputStream output = new GZIPOutputStream(dataStreams.output())) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
            context.getLogger().info("PUT - Executed Streamlet: " + name + ", as part of pipeline: {}", context.getPolicy().getPipeline());
        } catch (IOException e) {
            throw new RuntimeException("Error compressing data", e);
        }
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        try (InputStream input = new GZIPInputStream(dataStreams.input());
             OutputStream output = dataStreams.output()) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
            context.getLogger().info("GET - Executed Streamlet: " + name + ", as part of pipeline: {}", context.getPolicy().getPipeline());
        } catch (IOException e) {
            throw new RuntimeException("Error decompressing data", e);
        }
    }
}
