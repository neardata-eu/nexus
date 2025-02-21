package io.nexus.streamlets.context;

import io.nexus.streamlets.metadata.S3StorageConfig;
import io.nexus.streamlets.utils.CachedS3Client;
import org.slf4j.Logger;
import io.nexus.streamlets.metadata.Policy;

import java.util.List;

/**
 * ContextManager is a singleton class that provides methods to manage
 * RequestContext and MultipartContext instances. ContextManager is a logical
 * wrapper around the StreamletContext interface, providing restricted user
 * access to the current streamlet context.
 * 
 * Ex: user should not be able to set the current StreamletContext's policy, so
 * package-specific setters are used, then called in this class
 */
public class ContextManager {

    private static final ContextManager instance = new ContextManager();

    private ContextManager() {}

    public static ContextManager getInstance() {
        return instance;
    }

    /**
     * Creates and returns a new RequestContext instance.
     *
     * @param logger the Logger instance of the execution
     * @param policy the current working Policy
     * @return a new RequestContext instance.
     */
    public RequestContext createRequestStreamletContext(Logger logger, Policy policy, String blobName,
                                                        List<S3StorageConfig> s3StorageConfigs, CachedS3Client cachedS3Client) {
        return new RequestContext(logger, policy, blobName, s3StorageConfigs, cachedS3Client);
    }
}