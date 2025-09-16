package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.DataSourceStreamlet;
import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SimpleCachingStreamlet extends ByteStreamlet implements DataSourceStreamlet {

    private final String name = "SIMPLE_CACHING";
    private final static String STORAGE_DIR = "/tmp/nexus-cache/";

    public SimpleCachingStreamlet() {
    }

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        String objectInStorage = STORAGE_DIR + context.getStreamPartition().getScopedPartitionUri();
        int totalBytesRead = 0;
        try (InputStream inputStream = dataStreams.input();
             OutputStream outputStream = dataStreams.output()) {
            createDirectoriesAndFile(objectInStorage);
            OutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(objectInStorage));
            int currentBytesRead = 0;
            byte[] target = new byte[8192];
            while ((currentBytesRead = inputStream.read(target)) != -1) {
                outputStream.write(target, 0, currentBytesRead);
                fileOutputStream.write(target, 0, currentBytesRead);
                totalBytesRead += currentBytesRead;
            }
            fileOutputStream.close();
            context.getLogger().info("Finished Streamlet {} PUT processing. Processed Bytes: {}", name, totalBytesRead);
        } catch (Exception  e) {
            context.getLogger().error("Error in Streamlet {}", name, e);
        }
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        throw new UnsupportedOperationException("Streamlet " + name + " is not supposed to implement GET processing.");
    }

    @Override
    public InputStream handlePreGet(StreamPartition streamPartition, StreamletContext context) {
        String objectInStorage = STORAGE_DIR + streamPartition.getScopedPartitionUri();
        File file = new File(objectInStorage);
        try {
            return file.getAbsoluteFile().exists() ? new BufferedInputStream(new FileInputStream(file)) : null;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createDirectoriesAndFile(String filePath) {
        try {
            // Create the directories and the file if they don't exist
            Path path = Paths.get(filePath);
            if (Files.exists(path)) {
                Files.delete(path);
            }
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
