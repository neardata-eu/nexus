package io.nexus.streamlets.context;

import io.nexus.streamlets.metadata.S3StorageConfig;
import io.nexus.streamlets.utils.CachedS3Client;
import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

import java.io.InputStream;
import java.util.List;

/**
 * Since regular PUT and GET requests operate on MutableBlobMetadata, and
 * Multipart events operate on BlobMetadata, this interface provides a layer of
 * abstraction for both events.
 * Each implementation should provide the policy, stream, logger, setters, and
 * getters for their respective metadata type.
 */
public interface StreamletContext {
    /**
     * Retrieves the policy for the current context.
     *
     * @return the policy.
     */
    Policy getPolicy();

    /**
     * Retrieves the logger for the current context.
     *
     * @return the logger.
     */
    Logger getLogger();

    List<S3StorageConfig> getS3StorageConfigs();

    void routeObjectToPolicyStorage(S3StorageConfig config, InputStream objectContent, long contentLength);

    String getUserMetadata(String key);

    String putUserMetadata(String key, String value);
}
