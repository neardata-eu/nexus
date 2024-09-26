package io.nexus.streamlets.utils;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MultipartUpload;

import java.io.File;

/**
 *  We assume that the name structure of chunks of tiered stream data is of
 *  the form scope/streamName/object.xxx
 */
public class StreamNameUtils {

    public static final String STREAM_SEPARATOR = "/";

    public static String getScopeFromRequest(MultipartUpload multipartUpload) {
        return getScopeFromChunkName(multipartUpload.blobName());
    }

    public static String getStreamFromRequest(MultipartUpload multipartUpload) {
        return getStreamFromChunkName(multipartUpload.blobName());
    }

    public static String getStreamFromRequest(Blob blob) {
        return getStreamFromChunkName(blob.getMetadata().getName());
    }

    public static String getScopeFromRequest(Blob blob) {
        return getScopeFromChunkName(blob.getMetadata().getName());
    }

    private static String getScopeFromChunkName(String chunkName) {
        return getChunkNameComponent(chunkName, 0);
    }

    private static String getStreamFromChunkName(String chunkName) {
        return getChunkNameComponent(chunkName, 1);
    }

    private static String getChunkNameComponent(String chunkName, int index) {
        if (chunkName == null || chunkName.isEmpty()) {
            return null;
        }
        String[] nameComponents = chunkName.split(STREAM_SEPARATOR);
        if (nameComponents.length < 2) {
            return null;
        }
        // Return the immediate component before the stream name.
        return nameComponents[index];
    }
}
