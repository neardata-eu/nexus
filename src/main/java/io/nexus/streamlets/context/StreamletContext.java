package io.nexus.streamlets.context;

import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.metadata.S3StorageConfig;
import io.nexus.streamlets.utils.CachedS3Client;
import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

import java.io.InputStream;
import java.util.List;

/**
 * Abstracts the context for a streamlet, providing a layer of abstraction for both mutable and immutable blob metadata
 * events. Implementations of this interface are responsible for providing the necessary metadata and configuration for
 * their respective use cases.
 */
public interface StreamletContext {
    /**
     * Retrieves the policy for the current context, defining the rules and behavior for the streamlet pipeline.
     *
     * @return the policy for the current context.
     */
    Policy getPolicy();

    /**
     * Retrieves the logger for the current context, used for logging events and errors within the streamlet.
     *
     * @return the logger for the current context.
     */
    Logger getLogger();

    /**
     * Retrieves the stream partition for the current request context.
     *
     * @return the stream partition for the current request context.
     */
    StreamPartition getStreamPartition();

    /**
     * Retrieves the S3 storage configurations for the current context.
     *
     * @return a list of S3 storage configurations for the current context.
     */
    List<S3StorageConfig> getS3StorageConfigs();

    /**
     * Routes an object to policy storage, handling the upload of the object.
     *
     * @param config the S3 storage configuration for the object.
     * @param objectContent the input stream for the object content.
     * @param contentLength the length of the object content.
     */
    void routeObjectToPolicyStorage(S3StorageConfig config, InputStream objectContent, long contentLength);

    /**
     * Fetches an object from policy storage, handling the download of the object.
     *
     * @param config the S3 storage configuration for the object.
     * @param streamPartition the stream partition for the object.
     * @return an input stream for the object content.
     */
    InputStream fetchObjectFromPolicyStorage(S3StorageConfig config, StreamPartition streamPartition);

    /**
     * Retrieves a user metadata value for the current context, defined by the key.
     *
     * @param key the key for the user metadata value.
     * @return the user metadata value for the specified key.
     */
    String getUserMetadata(String key);

    /**
     * Puts a user metadata value for the current context, defined by the key and value.
     *
     * @param key the key for the user metadata value.
     * @param value the value for the user metadata.
     * @return the user metadata value for the specified key.
     */
    String putUserMetadata(String key, String value);
}