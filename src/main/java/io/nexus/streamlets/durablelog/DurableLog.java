package io.nexus.streamlets.durablelog;

public interface DurableLog {

    public boolean createLogObject(String logObjectName);

    public void writeToLogObject(String logObjectName, byte[] data, int bytesRead);

    public boolean closeLogObject(String logObjectName);
}
