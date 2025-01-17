package io.nexus.streamlets.state;

import java.lang.reflect.Field;

/**
 * Manager class for managing persistent data structures
 * in Streamlets. It provides convenience methods for
 * loading and saving the state of Streamlets before and
 * after their execution, respectively.
 */
public class StreamletStateManager {

    private final StreamletStateBackend backend;

    public StreamletStateManager(StreamletStateBackend backend) {
        this.backend = backend;
    }

    /**
     * Loads persistent data structures in a given object.
     *
     * @param instance Object with data structures to be loaded.
     */
    public void loadPersistentFields(Object instance) {
        Class<?> clazz = instance.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Persistent.class)) {
                Persistent annotation = field.getAnnotation(Persistent.class);
                // TODO: For the partitioned case, we need to know the partition name, which should be added in
                //  the streamlet (but we depend on completing the refactoring the API to get there)
                String key = annotation.name();
                field.setAccessible(true);
                try {
                    Object value = this.backend.load(key, field.getType());
                    if (value != null) {
                        field.set(instance, value);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to load persistent field", e);
                }
            }
        }
    }

    /**
     * Saves persistent data structures in a given object.
     *
     * @param instance Object with data structures to be saved.
     */
    public void savePersistentFields(Object instance) {
        Class<?> clazz = instance.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Persistent.class)) {
                Persistent annotation = field.getAnnotation(Persistent.class);
                String key = annotation.name();
                field.setAccessible(true);
                try {
                    Object value = field.get(instance);
                    this.backend.save(key, value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to save persistent field", e);
                }
            }
        }
    }
}

