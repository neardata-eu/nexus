package io.nexus.streamlets.utils;

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
 * Creates S3 clients to different endpoints and keeps them cached for futher reuse.
 */
public class CachedS3Client implements Closeable {

    private final Map<String, BlobStoreContext> contextCache = new ConcurrentHashMap<>();

    public void routeObjectTo(String endpoint, InputStream objectContent, String accessKey, String secretKey,
                              String container, String blobName) {
        BlobStore blobStore = this.contextCache.computeIfAbsent(endpoint, key -> createContext(key, accessKey, secretKey))
                .getBlobStore();
        uploadBlob(blobStore, objectContent, container, blobName);
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

    private void uploadBlob(BlobStore blobStore, InputStream inputStream, String container, String blobName) {
        Blob blob = null;
        try {
            blob = blobStore.blobBuilder(blobName)
                    .payload(Payloads.newInputStreamPayload(inputStream))
                    .contentLength(5242880)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        blobStore.putBlob(container, blob);
        System.out.println("Uploaded blob: " + blobName);
    }

    public void close() {
        contextCache.values().forEach(BlobStoreContext::close);
        contextCache.clear();
    }
}

