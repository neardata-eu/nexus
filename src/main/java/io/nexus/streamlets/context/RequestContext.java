package io.nexus.streamlets.context;

import com.google.common.collect.ImmutableMap;
import io.nexus.streamlets.Streamlet;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.StreamletDescriptor;
import io.nexus.streamlets.metadata.StreamletExecutionDescriptor;
import io.nexus.streamlets.utils.ObjectTagsUtils;
import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
        if (key != null && ObjectTagsUtils.isSystemKey(key)) {
            throw new IllegalArgumentException("This metadata key uses a reserved prefix, choose another one.");
        }
        return this.metadata.put(key.toLowerCase(), value.toLowerCase());
    }

    @Override
    public String getUserMetadata(String key) {
        // Return the value associated with the key, or null if the key does not exist
        return this.metadata.getOrDefault(key.toLowerCase(), null);
    }

    public Map<String, String> getUserMetadataCopy() {
        return ImmutableMap.copyOf(this.metadata);
    }

    public void populateUserMetadata(Map<String, String> requestMetadata) {
        if (requestMetadata != null && !requestMetadata.isEmpty()) {
            requestMetadata.forEach((k, v) -> logger.info("Populating request metadata: {},{}", k, v));
            this.metadata.putAll(requestMetadata);
        }
    }                                              

    @Override
    public String toString() {
        return "{ logger='" + logger + "', policy='" + policy + " }";
    }

    public void addTransformerStreamletsToMetadata(Region thisRegion) {
        String transformerStreamlets = ObjectTagsUtils.encodeTransformerStreamletsTag(thisRegion, this.policy);
        if (transformerStreamlets != null && !transformerStreamlets.isEmpty()) {
            // Store the transformer Streamlets as a reserved system metadata tag.
            this.metadata.put(ObjectTagsUtils.getSystemTransformerStreamletsPrefix(), transformerStreamlets);
        }
    }
}
