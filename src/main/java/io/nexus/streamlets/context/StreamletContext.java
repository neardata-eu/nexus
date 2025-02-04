package io.nexus.streamlets.context;

import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

/**
 * Since regular PUT and GET requests operate on MutableBlobMetadata, and
 * Multipart events operate on BlobMetadata, this interface provides a layer of
 * abstraction for both events.
 * 
 * Each implementation should provide the policy, stream, logger, setters, and
 * getters for their respective metadata type
 * 
 * TODO: Revisit this after multipart support changes
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

    /**
     * Gets all headers for the current context
     * 
     */

    String getUserMetadata(String key);

    /**
     * Sets custom headers for the current context
     *
     * @param headers the list of headers to set, where each header is represented
     *                by a MetadataPair.
     */
    String putUserMetadata(String key, String value);

}
