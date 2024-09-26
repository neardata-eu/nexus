package io.nexus.streamlets.durablelog;

import io.nexus.streamlets.StreamPartitionPojo;

import java.io.IOException;

/**
 * Interface that defines the operations for a durable log in Nexus that stores data upon
 * the arrival of a storage request for reliability purposes.
 *
 */
public interface DurableLog {

    public boolean createLogObject(StreamPartitionPojo streamPartitionPojo);

    public void writeToLogObject(String logObjectName, byte[] data, int bytesRead);

    public void closeLogObject(String logObjectName) throws IOException;
}
