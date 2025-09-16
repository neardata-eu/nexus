package io.nexus.streamlets.utils;

import com.google.common.net.HttpHeaders;
import io.nexus.streamlets.ForwardedRequestException;
import io.nexus.streamlets.StreamletsMetrics;
import io.nexus.streamlets.context.RequestContext;
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

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

    public RequestManager(ForwardingBlobStore blobStore, ScheduledExecutorService dataTransferExecutor) {
        this.blobStore = blobStore;
        this.client = HttpClients.createDefault();
        this.dataTransferExecutor = dataTransferExecutor;
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
                            logger.info("Multi-part upload to be forwarded or re-routed, continue as normal.");
                            return null;
                        } else {
                            throw new RuntimeException(ex);
                        }
                    }));
        }
    }

    public CompletableFuture<Void> doAsyncGetRequest(String containerName, String blobName, GetOptions getOptions,
                                                      OutputStream streamletInputOutputStream, RequestContext context) {
        long startTime = System.nanoTime();
        Blob proxyBlob = (getOptions == null) ? this.blobStore.getContext().getBlobStore().getBlob(containerName, blobName) :
                this.blobStore.getContext().getBlobStore().getBlob(containerName, blobName, getOptions);
        if (proxyBlob == null) {
            //FIXME: In some concurrency situations proxyBlob may be null, need to better handle this case.
            logger.error("Null blob from blobStore, just returning.");
            return CompletableFuture.completedFuture(null);
        }
        // FIll the context with metadata synchronously to make it available right away to streamlets.
        context.populateUserMetadata(proxyBlob.getMetadata().getUserMetadata());
        // The actual data transfer occurs asynchronously.
        return CompletableFuture.runAsync(() -> {
            try (InputStream is = proxyBlob.getPayload().openStream()){
                int readBytes;
                long totalBytesTransferred = 0;
                byte[] content = new byte[16 * 1024];
                while ((readBytes = is.read(content)) != -1) {
                    streamletInputOutputStream.write(content, 0, readBytes);
                    totalBytesTransferred += readBytes;
                }
                streamletInputOutputStream.close();
                // Record metrics.
                double operationTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
                StreamletsMetrics.GET_REQUEST_STORAGE_OPERATIONS_COUNTER.incrementCounter();
                double transferSizeMB = (totalBytesTransferred / (1024.0 * 1024.0));
                StreamletsMetrics.GET_REQUEST_STORAGE_SIZE_GAUGE.record(transferSizeMB);
                double transferSpeedMBps = transferSizeMB / operationTimeSeconds;
                StreamletsMetrics.GET_REQUEST_STORAGE_THROUGHPUT_GAUGE.record(transferSpeedMBps);
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
        long startTime = System.nanoTime();
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
            // FIXME: At least for Pravega, this thing seems to put double // that make S3 requests to fail
            this.blobStore.copyBlob(containerName, blobName, containerName, blobName, copyOptions);
            // Record metrics.
            long operationTime = System.nanoTime() - startTime;
            StreamletsMetrics.METADATA_TAGS_UPDATE_OPERATIONS_COUNTER.incrementCounter();
            StreamletsMetrics.METADATA_TAGS_UPDATE_LATENCY_TIMER.record(operationTime);
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
        long startTime = System.nanoTime();
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
                    // Record metrics.
                    double operationTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
                    StreamletsMetrics.GET_REQUEST_FORWARD_OPERATIONS_COUNTER.incrementCounter();
                    double transferSizeMB = (transferredBytes / (1024.0 * 1024.0));
                    StreamletsMetrics.GET_REQUEST_FORWARD_SIZE_GAUGE.record(transferSizeMB);
                    double transferSpeedMBps = transferSizeMB / operationTimeSeconds;
                    StreamletsMetrics.GET_REQUEST_FORWARD_THROUGHPUT_GAUGE.record(transferSpeedMBps);
                    logger.info("Completed forward GET to {}, available bytes {}", targetUrl, transferredBytes);
                } catch (IOException e) {
                    throw new RuntimeException("I/O error while processing GET response for " + targetUrl, e);
                }
            }, this.dataTransferExecutor);
        } catch (Exception e) {
            throw new RuntimeException("Error while forwarding request", e);
        }
    }
    
    public String buildCuratedTargetUrl(String targetServerUrl, String container, String blobName) throws UnsupportedEncodingException {
        String curatedTargetURL = !targetServerUrl.startsWith(HTTP_URL_PREFIX) ?
                HTTP_URL_PREFIX + targetServerUrl : targetServerUrl;
        curatedTargetURL += URLEncoder.encode(container, StandardCharsets.UTF_8) + "/" + URLEncoder.encode(blobName, StandardCharsets.UTF_8);
        return curatedTargetURL;
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
        long startTime = System.nanoTime();
        return CompletableFuture.runAsync(() -> {
            try {
                // Construct the target URL
                String curatedTargetURL = !targetServerUrl.startsWith(HTTP_URL_PREFIX) ?
                        HTTP_URL_PREFIX + targetServerUrl : targetServerUrl;
                // Encode the blob name to handle special characters
                String blobName = blob.getMetadata().getName();
                String encodedBlobName = URLEncoder.encode(blobName, StandardCharsets.UTF_8)
                    .replace("+", "%20");
                String targetUrl = curatedTargetURL + container + "/" + encodedBlobName;
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
                    byte[] buffer = new byte[64 * 1024];
                    int bytesRead;
                    long totalBytesForwarded = 0;
                    while ((bytesRead = processedContent.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                        totalBytesForwarded += bytesRead;
                    }
                    // Record metrics.
                    double operationTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
                    StreamletsMetrics.PUT_REQUEST_FORWARD_OPERATIONS_COUNTER.incrementCounter();
                    double transferSizeMB = (totalBytesForwarded / (1024.0 * 1024.0));
                    StreamletsMetrics.PUT_REQUEST_FORWARD_SIZE_GAUGE.record(transferSizeMB);
                    double transferSpeedMBps = transferSizeMB / operationTimeSeconds;
                    StreamletsMetrics.PUT_REQUEST_FORWARD_THROUGHPUT_GAUGE.record(transferSpeedMBps);
                    logger.info("Completed PUT forward with {} MBps of {}", transferSpeedMBps, targetUrl);
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
