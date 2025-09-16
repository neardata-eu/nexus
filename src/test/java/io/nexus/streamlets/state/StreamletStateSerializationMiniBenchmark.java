package io.nexus.streamlets.state;

import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.state.backends.RocksDBStateBackend;
import org.junit.jupiter.api.Test;

public class StreamletStateSerializationMiniBenchmark {

    @Test
    public void testSerializationOverhead() {
        StreamletStateBackend backend = new RocksDBStateBackend("/tmp/nexus"); // new InMemoryStateBackend();
        StreamletStateManager manager = new StreamletStateManager(backend);

        SharedPersistentMapTestFunction myFunction = new SharedPersistentMapTestFunction();
        StreamPartition streamPartition = new StreamPartition("container", "scope", "stream", "partition");

        // Generate a Map of some entries and check the speed of storing and loading
        fillFunctionMap(1000, myFunction);
        long iniTime = 0;
        int iterations = 10000;
        for (int i = 0; i < iterations; i++) {
            manager.savePersistentFields(myFunction, streamPartition);
            manager.loadPersistentFields(myFunction, false, streamPartition);
            // Remove initial time artifact from time measurement
            if (iniTime == 0) {
                iniTime = System.nanoTime();
            }
        }
        System.err.println(
                "Total elapsed time (ms) managing streamlet state " + (((System.nanoTime() - iniTime) / 1000000.0)));
        System.err.println("Per iteration elapsed time (ms) managing streamlet state "
                + (((System.nanoTime() - iniTime) / 1000000.0) / iterations));

    }

    public static void fillFunctionMap(int numberOfEntries, SharedPersistentMapTestFunction myFunction) {
        for (int i = 1; i <= numberOfEntries; i++) {
            myFunction.apply("Key" + i, "Value" + i);
        }
    }
}
