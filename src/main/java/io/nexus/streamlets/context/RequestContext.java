package io.nexus.streamlets.context;

import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class for handling the context for normal PUT and GET requests
 */
public class RequestContext implements StreamletContext {
    private final Logger logger;
    private final Policy policy;
    private final Map<String, String> metadata;

    public RequestContext(Logger logger, Policy policy) {
        this.logger = logger;
        this.policy = policy;
        this.metadata = new ConcurrentHashMap<>();
    }

    @Override
    public Policy getPolicy() {
        return policy;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public String putUserMetadata(String key, String value) {
        return this.metadata.put(key.toLowerCase(), value.toLowerCase());
    }

    @Override
    public String getUserMetadata(String key) {
        // Return the value associated with the key, or null if the key does not exist
        return this.metadata.getOrDefault(key.toLowerCase(), null);
    }

    @Override
    public String toString() {
        return "{ logger='" + logger + "', policy='" + policy + " }";
    }
}
