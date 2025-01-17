package io.nexus.streamlets.state;

/**
 * Interface for the storage backends that store state of Streamlets
 * annotated with {@link Persistent}.
 */
public interface StreamletStateBackend {

    /**
     * Stores the state of a data structure in the backend.
     *
     * @param key Name of the data structure.
     * @param value Value of the data structure.
     * @param <T> Type of the data structure.
     */
    <T> void save(String key, T value);

    /**
     * Loads the state of a data structure from the backend.
     *
     * @param key Name of the data structure.
     * @param type Type of the data structure.
     * @return Loaded data structure.
     * @param <T> Type of the data structure.
     */
    <T> T load(String key, Class<T> type);

    /**
     * Deletes a data structure stored in the backend.
     *
     * @param key Name of the data structure.
     */
    void delete(String key);
}