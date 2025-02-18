package io.nexus.streamlets.utils;

import com.google.common.net.HttpHeaders;
import io.nexus.streamlets.ForwardedRequestException;
import io.nexus.streamlets.context.RequestContext;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import jakarta.ws.rs.core.Response;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.io.Payloads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Class that contains the logic for managing HTTP request against an S3-compliant server.
 */
public class RequestManager implements Closeable {

    private static final String HTTP_URL_PREFIX = "http://";
    private final Logger logger = LoggerFactory.getLogger(RequestManager.class);
    private final ForwardingBlobStore blobStore;
    private final CloseableHttpClient client;
    private final ScheduledExecutorService dataTransferExecutor;

    public RequestManager(ForwardingBlobStore blobStore) {
        this.blobStore = blobStore;
        this.client = HttpClients.createDefault();
        this.dataTransferExecutor = ExecutorServiceHelpers.newScheduledThreadPool(40, "data-transfer-threadpool");
    }

    public void initiatePutRequestFromMultipartUpload(MultiPartUploadState uploadState) {
        // Check if this is the first part for this upload.
        if (uploadState.isUploadInitialized().compareAndSet(false, true)) {
            logger.info("Creating blob metadata {}.", uploadState.getUploadId());
            // Create a blob with the metadata from the original request
            Blob blob = this.blobStore.blobBuilder(uploadState.getBlobMetadata().getName())
                    .payload(Payloads.newInputStreamPayload(uploadState.getInputStream()))
                    .contentLength(uploadState.getMultipartUploadSize())
                    .contentType(uploadState.getBlobMetadata().getContentMetadata().getContentType())
                    .build();

            // The first time that we upload a part, in addition to transferring the data, we have to initiate the PUT.
            uploadState.setPutRequest(CompletableFuture.runAsync(() ->
                            this.blobStore.putBlob(uploadState.getContainer(), blob), this.dataTransferExecutor)
                    .exceptionally(ex -> {
                        // The created PUT may need to be forwarded, which is fine.
                        if (ex.getCause() instanceof ForwardedRequestException) {
                            logger.info("Multi-part upload to be forwarded, continue as normal.");
                            return null;
                        } else {
                            throw new RuntimeException(ex);
                        }
                    }));
        }
    }

    public CompletableFuture<Void> doAsyncGetRequest(String containerName, String blobName, GetOptions getOptions,
                                                      OutputStream streamletInputOutputStream, RequestContext context) {
        Blob proxyBlob = (getOptions == null) ? this.blobStore.getContext().getBlobStore().getBlob(containerName, blobName) :
                this.blobStore.getContext().getBlobStore().getBlob(containerName, blobName, getOptions);
        // FIll the context with metadata synchronously to make it available right away to streamlets.
        context.populateUserMetadata(proxyBlob.getMetadata().getUserMetadata());
        // The actual data transfer occurs asynchronously.
        return CompletableFuture.runAsync(() -> {
            try (InputStream is = proxyBlob.getPayload().openStream()){
                int readBytes;

                byte[] content = new byte[16 * 1024];
                while ((readBytes = is.read(content)) != -1) {
                    streamletInputOutputStream.write(content, 0, readBytes);
                }
                streamletInputOutputStream.close();
                logger.info("Completed GET request for {} / {}.", containerName, blobName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, this.dataTransferExecutor);
    }

    /**
     * Asynchronously updates only the metadata of a blob without fetching the entire object.
     *
     * @param containerName The name of the container.
     * @param blobName      The name of the blob.
     * @param context       The context with the metadata key-value pairs to store.
     * @return A CompletableFuture that completes when the metadata update is done.
     */
    public CompletableFuture<Void> updateMetadataAsync(String containerName, String blobName, RequestContext context) {
        Map<String, String> userMetadata = context.getUserMetadataCopy();
        if (userMetadata.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(() -> {
            BlobMetadata blobMetadata = this.blobStore.blobMetadata(containerName, blobName);
            if (blobMetadata == null) {
                throw new RuntimeException("Blob not found: " + blobName);
            }
            Map<String, String> mergedTags = new HashMap<>(blobMetadata.getUserMetadata());
            mergedTags.putAll(userMetadata);
            CopyOptions copyOptions = CopyOptions.builder().userMetadata(mergedTags).build();
            logger.info("Saving user metadata as object tags ({}), total tags ({}).", userMetadata.size(), mergedTags.size());
            this.blobStore.copyBlob(containerName, blobName, containerName, blobName, copyOptions);

        }, this.dataTransferExecutor);
    }

    /**
     * Forward a GET request and retrieve the blob.
     *
     * @param targetServerUrl The endpoint to forward the GET request.
     * @param container The container name.
     * @param blobName  The blob name to forward.
     * @return A CompletableFuture representing the operation and the retrieved blob.
     */
    public CompletableFuture<Void> forwardGetRequest(String targetServerUrl, String container, String blobName,
                                                     OutputStream getContents, RequestContext context) {
        try {
            // Construct the target URL
            String targetUrl = buildCuratedTargetUrl(targetServerUrl, container, blobName);
            logger.info("Starting forward GET to {}.", targetUrl);
            HttpResponse response = doGetRequest(targetUrl);
            // Populate context with user metadata tags.
            context.populateUserMetadata(ObjectTagsUtils.extractUserMetadataFromHeaders(response));
            return CompletableFuture.runAsync(() -> {
                // Read the response body and construct a blob
                HttpEntity entity = response.getEntity();
                try (InputStream inputStream = entity.getContent()) {
                    long transferredBytes = inputStream.transferTo(getContents);
                    getContents.close(); // Close the stream used as GET contents input.
                    logger.info("Completed forward GET to {}, available bytes {}", targetUrl, transferredBytes);
                } catch (IOException e) {
                    throw new RuntimeException("I/O error while processing GET response for " + targetUrl, e);
                }
            }, this.dataTransferExecutor);
        } catch (Exception e) {
            throw new RuntimeException("Error while forwarding request", e);
        }
    }
    
    public String buildCuratedTargetUrl(String targetServerUrl, String container, String blobName) {
        String curatedTargetURL = !targetServerUrl.startsWith(HTTP_URL_PREFIX) ?
                HTTP_URL_PREFIX + targetServerUrl : targetServerUrl;
        return curatedTargetURL + container + "/" + blobName;
    }

    public HttpResponse doGetRequest(String targetUrl) {
        HttpGet request = new HttpGet(targetUrl);
        HttpResponse response = null;
        try {
            response = client.execute(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int responseCode = response.getStatusLine().getStatusCode();
        if (responseCode >= 200 && responseCode < 300) {
            return response;
        } else {
            throw new RuntimeException("Failed to forward request, HTTP code: " + responseCode);
        }
    }

    /**
     * Forward PUT requests asynchronously.
     *
     * @param targetServerUrl    Target server URL.
     * @param container The container name.
     * @param blob      The blob to forward.
     * @return A CompletableFuture representing the operation.
     */
    public CompletableFuture<Void> forwardPutRequest(String targetServerUrl, String container, Blob blob,
                                                      InputStream processedContent) {
        return CompletableFuture.runAsync(() -> {
            try {
                // Construct the target URL
                String curatedTargetURL = !targetServerUrl.startsWith(HTTP_URL_PREFIX) ?
                        HTTP_URL_PREFIX + targetServerUrl : targetServerUrl;
                String targetUrl = curatedTargetURL + container + "/" + blob.getMetadata().getName();
                long contentLength = blob.getMetadata().getContentMetadata().getContentLength();
                logger.info("Started forwarding PUT ({} bytes) to {}.", contentLength, targetUrl);
                URL url = new URL(targetUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("PUT");
                connection.setDoOutput(true);

                // Set headers from blob metadata
                blob.getMetadata().getUserMetadata().forEach(connection::setRequestProperty);
                connection.setRequestProperty(HttpHeaders.CONTENT_TYPE,
                        blob.getMetadata().getContentMetadata().getContentType());
                connection.setRequestProperty(HttpHeaders.CONTENT_LENGTH, String.valueOf(contentLength));

                // Write the payload
                try (OutputStream outputStream = connection.getOutputStream()) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    long totalBytesForwarded = 0;
                    long startTime = System.currentTimeMillis();
                    while ((bytesRead = processedContent.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                        totalBytesForwarded += bytesRead;
                    }
                    logger.info("Completed PUT forward with {} MBps of {}", (totalBytesForwarded / (1024.0 * 1024.0)) /
                            ((System.currentTimeMillis() - startTime) / 1000.0), targetUrl);
                }
                // Handle the response
                int responseCode = connection.getResponseCode();
                if (responseCode < 200 || responseCode >= 300) {
                    throw new RuntimeException("Failed to forward request, HTTP code: " + responseCode);
                }
                logger.info("Completed forward PUT to {}", targetUrl);
            } catch (Exception e) {
                throw new RuntimeException("Error while forwarding request", e);
            }
        }, this.dataTransferExecutor);
    }

    @Override
    public void close() throws IOException {
        this.client.close();
        this.dataTransferExecutor.shutdown();
    }
}
