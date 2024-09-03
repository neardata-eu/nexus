package io.nexus.streamlets.durablelog;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileSystemDurableLog implements DurableLog {

    private final Map<String, OutputStream> logObjectWriters = new HashMap<>();

    @Override
    public boolean createLogObject(String logObjectName) {
        File file = new File(logObjectName);
        try {
            boolean created = file.createNewFile();
            this.logObjectWriters.put(logObjectName, new BufferedOutputStream(new FileOutputStream(logObjectName)));
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
