package io.nexus.streamlets.state;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class AbstractFunctionStateTest {

    protected StreamletStateManager manager;
    protected StreamletStateBackend backend;

    @Test
    public void testBasicDataStructurePersistence() {
        PersistentMapTestFunction myFunction = new PersistentMapTestFunction();
        // Load persisted state
        this.manager.loadPersistentFields(myFunction);
        // Update the map
        Assertions.assertNull(myFunction.apply("A", "1"));
        // Save the first value in the map, so the previous state is null
        manager.savePersistentFields(myFunction);

        // Start a second function
        PersistentMapTestFunction myFunction2 = new PersistentMapTestFunction();
        // Load persisted state
        manager.loadPersistentFields(myFunction2);
        // Make sure that we recover the previous state
        Assertions.assertEquals("1", myFunction2.apply("A", "2"));
        manager.savePersistentFields(myFunction2);
    }
}
