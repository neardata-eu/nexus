package io.nexus.streamlets.state;

import io.nexus.streamlets.StreamPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class AbstractFunctionStateTest {

    protected StreamletStateManager manager;
    protected StreamletStateBackend backend;

    @Test
    public void testSharedDataStructurePersistence() {
        SharedPersistentMapTestFunction myFunction = new SharedPersistentMapTestFunction();
        StreamPartition streamPartition = new StreamPartition("container", "scope", "stream",
                "partition");
        // Load persisted state
        this.manager.loadPersistentFields(myFunction, false, streamPartition);
        // Update the map
        Assertions.assertNull(myFunction.apply("A", "1"));
        // Save the first value in the map, so the previous state is null
        manager.savePersistentFields(myFunction, streamPartition);

        // Start a second function on the same scope/stream but for a different partition
        StreamPartition streamPartition2 = new StreamPartition("container", "scope", "stream",
                "partition2");
        SharedPersistentMapTestFunction myFunction2 = new SharedPersistentMapTestFunction();
        // Load persisted state
        manager.loadPersistentFields(myFunction2, false, streamPartition2);
        // Make sure that we recover the previous state
        Assertions.assertEquals("1", myFunction2.apply("A", "2"));
        manager.savePersistentFields(myFunction2, streamPartition2);

        // Start a third function on another stream
        StreamPartition streamPartition3 = new StreamPartition("container", "scope", "stream1",
                "partition");
        SharedPersistentMapTestFunction myFunction3 = new SharedPersistentMapTestFunction();
        // Load persisted state
        manager.loadPersistentFields(myFunction3, false, streamPartition3);
        // Make sure that we recover the previous state
        Assertions.assertNull(myFunction3.apply("A", "1"));
        manager.savePersistentFields(myFunction3, streamPartition3);
    }

    @Test
    public void testPartitionedDataStructurePersistence() {
        PartitionedPersistentMapTestFunction myFunction = new PartitionedPersistentMapTestFunction();
        StreamPartition streamPartition = new StreamPartition("container", "scope",
                "stream", "partition");
        // Load persisted state
        this.manager.loadPersistentFields(myFunction, false, streamPartition);
        // Update the map
        Assertions.assertNull(myFunction.apply("A", "1"));
        // Save the first value in the map, so the previous state is null
        manager.savePersistentFields(myFunction, streamPartition);

        // Start a second function on the very same partition
        PartitionedPersistentMapTestFunction myFunction2 = new PartitionedPersistentMapTestFunction();
        // Load persisted state
        manager.loadPersistentFields(myFunction2, false, streamPartition);
        // Make sure that we recover the previous state
        Assertions.assertEquals("1", myFunction2.apply("A", "2"));
        manager.savePersistentFields(myFunction2, streamPartition);

        // Start a third function on another stream
        StreamPartition streamPartition2 = new StreamPartition("container", "scope", "stream",
                "partition2");
        PartitionedPersistentMapTestFunction myFunction3 = new PartitionedPersistentMapTestFunction();
        // Load persisted state
        manager.loadPersistentFields(myFunction3, false, streamPartition2);
        // Make sure that we recover the previous state
        Assertions.assertNull(myFunction3.apply("A", "1"));
        manager.savePersistentFields(myFunction3, streamPartition2);
    }
}
