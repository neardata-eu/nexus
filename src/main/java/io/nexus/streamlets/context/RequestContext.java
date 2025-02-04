package io.nexus.streamlets.context;

import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

import java.util.Map;

import org.jclouds.blobstore.domain.Blob;

// A class for handling the context for normal PUT and GET requests 
public class RequestContext implements StreamletContext {
    private Logger logger;
    private Policy policy;
    private Blob blob;

    public RequestContext() {

    }

    public RequestContext(Logger logger, Policy policy, Blob blob) {
        this.logger = logger;
        this.policy = policy;
        this.blob = blob;
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

    @Override
    public String putUserMetadata(String key, String value) {
        Map<String, String> metadata = this.blob.getMetadata().getUserMetadata();
        String response = metadata.put(key.toLowerCase(), value.toLowerCase());
        blob.getMetadata().setUserMetadata(metadata);

        return response;
    }

    @Override
    public String getUserMetadata(String key) {
        Map<String, String> metadata = blob.getMetadata().getUserMetadata();
        // Return the value associated with the key, or null if the key does not exist
        return metadata.getOrDefault(key.toLowerCase(), null);
    }

    @Override
    public String toString() {
        return "{ logger='" + logger + "', policy='" + policy + "', blobName='" + blob.getMetadata().getName() + "'}";
    }
}
