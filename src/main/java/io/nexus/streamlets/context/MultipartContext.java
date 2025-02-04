package io.nexus.streamlets.context;

import java.util.List;
import java.util.Map;

import org.jclouds.blobstore.domain.BlobMetadata;
import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

/**
 * The MultipartContext class implements the StreamletContext interface and
 * provides the context for multipart upload events. The main difference between
 * MultipartContext and RequestContext is how the custom headers are handled.
 * Multipart events, unlike normal requests, operate on metadata during
 * event.initiateMultipartUpload(), and applies said metadata to all parts.
 * 
 * TODO: After findings regarding the multipart event's workings, this class
 * will be revisited in future work for multipart context handling, if needed
 */
public class MultipartContext implements StreamletContext {
    private Logger logger;
    private Policy policy;
    private BlobMetadata blobMetadata;

    public MultipartContext() {

    }

    public MultipartContext(Logger logger, Policy policy, BlobMetadata blobMetadata) {
        this.logger = logger;
        this.policy = policy;
        this.blobMetadata = blobMetadata;
    }

    @Override
    public Policy getPolicy() {
        return policy;
    }

    // Package-specific
    void setPolicy(Policy policy) {
        this.policy = policy;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    /// ------------------
    // These functions won't be reached as their are no MultipartContext objects
    // currently created
    // Dummy overriding to accommodate the interface for now
    @Override
    public String putUserMetadata(String key, String value) {
        return null;
    }

    @Override
    public String getUserMetadata(String key) {
        Map<String, String> metadata = blobMetadata.getUserMetadata();
        // Return the value associated with the key, or null if the key does not exist
        return metadata.getOrDefault(key, null);
    }
    /// ------------------

    @Override
    public String toString() {
        return "{ logger='" + logger + "', policy='" + policy + "', blobName='" + blobMetadata.getName() + "'}";
    }

}
