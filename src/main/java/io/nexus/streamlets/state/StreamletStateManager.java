package io.nexus.streamlets.state;

import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.StreamletsMetrics;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Manager class for managing persistent data structures in Streamlets. It provides convenience methods for
 * loading and saving the state of Streamlets before and* after their execution, respectively.
 */
public class StreamletStateManager {

    private final StreamletStateBackend backend;
    private static final String LOG_FILE_PATH = "/tmp/metadata-accesses.txt";

    public StreamletStateManager(StreamletStateBackend backend) {
        this.backend = backend;
    }

    /**
     * Loads persistent data structures in a given object.
     *
     * @param instance Object with data structures to be loaded.
     */
    public void loadPersistentFields(Object instance, boolean isCachedStreamlet, StreamPartition streamPartition) {
        Class<?> clazz = instance.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Persistent.class)) {
                Persistent annotation = field.getAnnotation(Persistent.class);
                StatePersistenceType persistenceType = annotation.type();
                field.setAccessible(true);
                // Only load the state of the Streamlet from the backend if it is the first time we use it or if it
                // not using partition locality.
                if (!isCachedStreamlet || persistenceType.equals(StatePersistenceType.SHARED)) {
                    try {
                        String key = buildMetadataKeyForStreamletObject(streamPartition, clazz, field.getName(), persistenceType);
                        Object value = this.backend.load(key, field.getType());
                        if (value != null) {
                            field.set(instance, value);
                        }
                        StreamletsMetrics.STREAMLET_STATE_READ_OPERATIONS_COUNTER.incrementCounter();
                        // FIXME: This is just for easy log collection, remove this log line after experiments.
                        logMetadataAccessToFile("r");
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Failed to load persistent field", e);
                    }
                }
            }
        }
    }

    /**
     * Saves persistent data structures in a given object.
     *
     * @param instance Object with data structures to be saved.
     */
    public void savePersistentFields(Object instance, StreamPartition streamPartition) {
        Class<?> clazz = instance.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Persistent.class)) {
                Persistent annotation = field.getAnnotation(Persistent.class);
                StatePersistenceType persistenceType = annotation.type();
                field.setAccessible(true);
                try {
                    String key = buildMetadataKeyForStreamletObject(streamPartition, clazz, field.getName(), persistenceType);
                    Object value = field.get(instance);
                    this.backend.save(key, value);
                    StreamletsMetrics.STREAMLET_STATE_WRITE_OPERATIONS_COUNTER.incrementCounter();
                    // FIXME: This is just for easy log collection, remove this log line after experiments.
                    logMetadataAccessToFile("w");
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to save persistent field", e);
                }
            }
        }
    }

    /**
     * Creates the identifier in metadata for a @Persistent data structure of a stateful streamlet. If the type of
     * persistence is StatePersistenceType.PARTITIONED, the system will create an individual data structure for the
     * metadata associated to each stream partition. This mode assumes that the system will route storage requests
     * to the right worker nodes based on consistent hashing. If the StatePersistenceType.SHARED is selected, there
     * will be a common data structure for all the streamlets executing operations for partitions related to a stream.
     *
     * @param streamPartition
     * @param clazz
     * @param fieldName
     * @param persistenceType
     * @return
     */
    String buildMetadataKeyForStreamletObject(StreamPartition streamPartition, Class<?> clazz, String fieldName,
                                                      StatePersistenceType persistenceType) {
        return (persistenceType.equals(StatePersistenceType.PARTITIONED)) ?
                streamPartition.getScopedPartitionUri().toLowerCase() + "-" + clazz.getName().toLowerCase() + "-" + fieldName.toLowerCase() :
                streamPartition.getScopedStreamName().toLowerCase() + "-" + clazz.getName().toLowerCase() + "-" + fieldName.toLowerCase();
    }

    private void logMetadataAccessToFile(String operationType) {
        String logLine = String.format("%s, %s%n", System.currentTimeMillis(), operationType);
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true)) {
            writer.write(logLine);
        } catch (IOException e) {
            // If logging fails, silently continue (don't break routing)
            System.err.println("Failed to write to metadata accesses log file." + e);
        }
    }
}

