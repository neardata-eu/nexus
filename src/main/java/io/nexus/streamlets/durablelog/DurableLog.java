package io.nexus.streamlets.durablelog;

import io.nexus.streamlets.StreamPartitionPojo;

import java.io.IOException;

public interface DurableLog {

    public boolean createLogObject(StreamPartitionPojo streamPartitionPojo);

    public void writeToLogObject(String logObjectName, byte[] data, int bytesRead);

    public void closeLogObject(String logObjectName) throws IOException;
}
