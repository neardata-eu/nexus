package io.nexus.streamlets.context;

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
    public RequestContext createRequestStreamletContext(Logger logger, Policy policy) {
        return new RequestContext(logger, policy);
    }
}