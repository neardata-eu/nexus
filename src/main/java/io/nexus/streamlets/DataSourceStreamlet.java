package io.nexus.streamlets;

import io.nexus.streamlets.context.StreamletContext;

import java.io.InputStream;

/**
 * The DataSourceStreamlet interface defines the contract for handling the content preload before a GET operation. This
 * may apply to Streamlets that cache data and can serve it locally, or to data routing Streamlets, which need to
 * dictate where the data is before starting a GET request.
 */
public interface DataSourceStreamlet {

    /**
     * Handles the preload of content before a GET operation. Implementing classes should define the specific logic
     * for handling this preload operation, which may involve loading the content from a local source.
     */
    public InputStream handlePreGet(StreamPartition streamPartition, StreamletContext context);
}
