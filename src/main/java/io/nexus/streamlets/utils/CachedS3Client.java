package io.nexus.streamlets.utils;

import io.nexus.streamlets.StreamPartition;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payloads;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates S3 clients to different endpoints and keeps them cached for further reuse.
 */
public class CachedS3Client implements Closeable {

    private final Map<String, BlobStoreContext> contextCache = new ConcurrentHashMap<>();

    public void routeObjectTo(String endpoint, InputStream objectContent, String accessKey, String secretKey,
                              String container, StreamPartition streamPartition, long contentLength) {
        BlobStore blobStore = this.contextCache.computeIfAbsent(endpoint, key -> createContext(key, accessKey, secretKey))
                .getBlobStore();
        uploadBlob(blobStore, objectContent, container, streamPartition, contentLength);
    }

    public InputStream fetchObjectFrom(String endpoint, String accessKey, String secretKey, String container,
                                       StreamPartition streamPartition) {
        BlobStore blobStore = this.contextCache.computeIfAbsent(endpoint, key -> createContext(key, accessKey, secretKey))
                .getBlobStore();
        return getBlobInputStream(blobStore, container, streamPartition);
    }

    private BlobStoreContext createContext(String endpoint, String accessKey, String secretKey) {
        Properties properties = new Properties();
        properties.setProperty("jclouds.s3.virtual-host-buckets", "false"); // Use path-style access

        return ContextBuilder.newBuilder("s3")
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .overrides(properties)
                .buildView(BlobStoreContext.class);
    }

    private void uploadBlob(BlobStore blobStore, InputStream inputStream, String container,
                            StreamPartition streamPartition, long contentLength) {
        Blob blob = null;
        try {
            blob = blobStore.blobBuilder(streamPartition.getScopedObjectName())
                    .payload(Payloads.newInputStreamPayload(inputStream))
                    .contentLength(contentLength)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        blobStore.putBlob(container, blob);
    }

    private InputStream getBlobInputStream(BlobStore blobStore, String containerName, StreamPartition streamPartition) {
        Blob blob = null;
        try {
            blob = blobStore.getContext().getBlobStore().getBlob(containerName, streamPartition.getScopedObjectName());
            return blob.getPayload().openStream();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        contextCache.values().forEach(BlobStoreContext::close);
        contextCache.clear();
    }
}

