package io.nexus.streamlets.context;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.slf4j.Logger;
import io.nexus.streamlets.metadata.Policy;

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

    private ContextManager() {

    }

    public static ContextManager getInstance() {
        return instance;
    }

    /**
     * Creates and returns a new RequestContext instance.
     *
     * @param logger the Logger instance of the execution
     * @param policy the current working Policy
     * @param blob   the Blob instance for the request context
     * @return a new RequestContext instance.
     */
    public RequestContext createRequestStreamletContext(Logger logger, Policy policy, Blob blob) {
        return new RequestContext(logger, policy, blob);
    }

    /**
     * Creates and returns a new MultipartContext instance.
     *
     * @param logger       the Logger instance of the execution
     * @param policy       the current working Policy
     * @param scope        the current scope
     * @param stream       the current stream identifier
     * @param blobMetadata the BlobMetadata for the multipart event
     * @return a new MultipartContext instance.
     */
    public MultipartContext createMultipartStreamletContext(Logger logger, Policy policy, BlobMetadata blobMetadata) {
        return new MultipartContext(logger, policy, blobMetadata);
    }

    // RequestContext package-specific methods
    public void setContextPolicy(RequestContext requestContext, Policy policy) {
        requestContext.setPolicy(policy);
    }

    // MultipartContext package-specific methods
    public void setContextPolicy(MultipartContext multipartContext, Policy policy) {
        multipartContext.setPolicy(policy);
    }

}