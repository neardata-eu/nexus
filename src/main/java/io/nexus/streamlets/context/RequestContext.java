package io.nexus.streamlets.context;

import com.google.common.collect.ImmutableMap;
import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.S3StorageConfig;
import io.nexus.streamlets.utils.CachedS3Client;
import io.nexus.streamlets.utils.ObjectTagsUtils;
import org.slf4j.Logger;

import io.nexus.streamlets.metadata.Policy;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class for handling the context for normal PUT and GET requests
 */
public class RequestContext implements StreamletContext {
    private final Logger logger;
    private final Policy policy;
    private final CachedS3Client cachedS3Client;
    private final StreamPartition streamPartition;
    List<S3StorageConfig> s3StorageConfigs;
    private final Map<String, String> metadata;

    public RequestContext(Logger logger, Policy policy, StreamPartition streamPartition,
                          List<S3StorageConfig> s3StorageConfigs, CachedS3Client cachedS3Client) {
        this.logger = logger;
        this.policy = policy;
        this.streamPartition = streamPartition;
        this.cachedS3Client = cachedS3Client;
        this.s3StorageConfigs = s3StorageConfigs;
        this.metadata = new ConcurrentHashMap<>();
    }

    @Override
    public Policy getPolicy() {
        return this.policy;
    }

    @Override
    public Logger getLogger() {
        return this.logger;
    }

    @Override
    public StreamPartition getStreamPartition() {
        return this.streamPartition;
    }

    @Override
    public List<S3StorageConfig> getS3StorageConfigs() {
        return this.s3StorageConfigs;
    }

    @Override
    public void routeObjectToPolicyStorage(S3StorageConfig config, InputStream objectContent, long contentLength) {
        this.cachedS3Client.routeObjectTo(config.getEndpoint(), objectContent, config.getAccessKey(),
                config.getSecretKey(), config.getContainer(), this.streamPartition, contentLength);
    }

    @Override
    public InputStream fetchObjectFromPolicyStorage(S3StorageConfig config, StreamPartition streamPartition) {
        return this.cachedS3Client.fetchObjectFrom(config.getEndpoint(), config.getAccessKey(),
                config.getSecretKey(), config.getContainer(), streamPartition);
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
