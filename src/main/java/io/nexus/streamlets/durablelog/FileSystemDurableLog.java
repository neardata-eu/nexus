package io.nexus.streamlets.durablelog;

import io.nexus.streamlets.StreamPartitionPojo;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileSystemDurableLog implements DurableLog {

    private final static String STORAGE_DIR = "/tmp/nexus-durablelog/";
    private final Map<String, OutputStream> logObjectWriters = new HashMap<>();

    @Override
    public boolean createLogObject(StreamPartitionPojo streamPartitionPojo) {
        try {
            String partitionDirInStorage = STORAGE_DIR + streamPartitionPojo.getScopedPartitionUri();
            File file = new File(partitionDirInStorage);
            boolean created = file.mkdirs();
            String partitionFileInStorage = partitionDirInStorage + File.separator + streamPartitionPojo.getObject();
            file = new File(partitionFileInStorage);
            created |= file.createNewFile();
            this.logObjectWriters.put(partitionFileInStorage, new BufferedOutputStream(new FileOutputStream(file)));
            return created;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeToLogObject(String logObjectName, byte[] data, int bytesRead) {
        try {
            this.logObjectWriters.get(logObjectName).write(data, 0, bytesRead);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeLogObject(String logObjectName) throws IOException {
        this.logObjectWriters.get(logObjectName).close();
    }
}
