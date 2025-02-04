package io.nexus.streamlets;

import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.InputStreamRecord;

/**
 * The Streamlet interface defines the contract for handling input stream
 * records in two different ways: via PUT and GET operations. Classes that
 * implement this interface must provide their own implementations for handling
 * these operations.
 */
public interface Streamlet {

    /**
     * Handles a PUT operation on the given input stream record. Implementing
     * classes should define the specific logic for handling the PUT operation.
     *
     * @param event   the record to be processed during the PUT operation. TODO:
     *                This will be extended upon in the event-based work
     * @param context the context in which the PUT operation is being handled,
     *                providing the user with read access to the context
     *                information, alongside read/write access to the blob's user
     *                metadata
     */
    public abstract void handlePut(InputStreamRecord event, StreamletContext context);

    /**
     * Handles a GET operation on the given input stream record. Implementing
     * classes should define the specific logic for handling the GET operation.
     *
     * @param event   the record to be processed during the GET operation. TODO:
     *                This will be extended upon in the event-based work
     * @param context the context in which the GET operation is being handled,
     *                providing the user with read access to the context
     *                information, alongside read/write access to the blob's user
     *                metadata
     */
    public abstract void handleGet(InputStreamRecord event, StreamletContext context);

}