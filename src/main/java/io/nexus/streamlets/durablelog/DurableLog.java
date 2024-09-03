package io.nexus.streamlets.durablelog;

import java.io.IOException;

public interface DurableLog {

    public boolean createLogObject(String logObjectName);

    public void writeToLogObject(String logObjectName, byte[] data, int bytesRead);

    public void closeLogObject(String logObjectName) throws IOException;
}
