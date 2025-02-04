package io.nexus.streamlets;

import com.google.common.net.HttpHeaders;
import io.nexus.streamlets.metadata.*;

import io.nexus.streamlets.utils.StreamNameUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.domain.Location;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * S3 Proxy middleware that intercepts storage requests and injects them into
 * {@link StreamletsExecutor}.
 */
public class StreamletsInterceptor extends ForwardingBlobStore {

    private static final String HTTP_URL_PREFIX = "http://";
    final Logger logger = LoggerFactory.getLogger(StreamletsInterceptor.class);
    private final StreamletsExecutor streamletsExecutor;

    // Abstracted timer for the whole multipart event
    private long multipartEventTime;
    private final MetadataService metadataService;

    private enum InterSwarmletRoutingType {
        INTRA_REGION, INTER_REGION, NO_INTER_SWARMLET_ROUTING
    }

    public StreamletsInterceptor(BlobStore blobStore, MetadataService metadataService) {
        super(blobStore);
        this.metadataService = metadataService;
        this.streamletsExecutor = new StreamletsExecutor(metadataService);
    }

    @Override
    protected BlobStore delegate() {
        return super.delegate();
    }

    @Override
    public BlobStoreContext getContext() {
        return super.getContext();
    }

    @Override
    public BlobBuilder blobBuilder(String name) {
        return super.blobBuilder(name);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return super.listAssignableLocations();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        return super.list();
    }

    @Override
    public boolean containerExists(String container) {
        return super.containerExists(container);
    }

    @Override
    public boolean createContainerInLocation(Location location, String container) {
        return super.createContainerInLocation(location, container);
    }

    @Override
    public boolean createContainerInLocation(Location location, String container,
            CreateContainerOptions createContainerOptions) {
        return super.createContainerInLocation(location, container, createContainerOptions);
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        return super.getContainerAccess(container);
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess containerAccess) {
        super.setContainerAccess(container, containerAccess);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
        return super.list(container);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container, ListContainerOptions options) {
        return super.list(container, options);
    }

    @Override
    public void clearContainer(String container) {
        super.clearContainer(container);
    }

    @Override
    public void clearContainer(String container, ListContainerOptions options) {
        super.clearContainer(container, options);
    }

    @Override
    public void deleteContainer(String container) {
        super.deleteContainer(container);
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        return super.deleteContainerIfEmpty(container);
    }

    @Override
    public boolean directoryExists(String container, String directory) {
        return super.directoryExists(container, directory);
    }

    @Override
    public void createDirectory(String container, String directory) {
        super.createDirectory(container, directory);
    }

    @Override
    public void deleteDirectory(String container, String directory) {
        super.deleteDirectory(container, directory);
    }

    @Override
    public boolean blobExists(String container, String name) {
        return super.blobExists(container, name);
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName,
            CopyOptions options) {
        return super.copyBlob(fromContainer, fromName, toContainer, toName, options);
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        return super.blobMetadata(container, name);
    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        return putBlob(containerName, blob, null);
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
        // 1. Extract the system, scope, and stream for the incoming request and get the
        // policy (if any).
        Policy policy;
        try {
            policy = checkRequestAndRetrievePolicy(containerName, blob);
        } catch (MalformedStreamStorageRequestException | NoPolicySetException e) {
            // Either the request is malformed or there are no policies, so just execute
            // operation against S3 endpoint.
            return super.putBlob(containerName, blob, putOptions);
        } catch (Exception ex) {
            // Unexpected error, rethrow.
            throw new RuntimeException(ex);
        }

        // 2. There is a policy set for the current stream. Let's be optimistic and
        // assume we can perform the processing
        // of the Streamlets in this Region (if any) checking first that we have the
        // right hardware available.
        Region currentRegion = this.metadataService.getNexusConfig().getRegion();
        Hardware availableHardware = this.metadataService.getNexusConfig().getHardware();
        tryProcessPut(policy, currentRegion, availableHardware, containerName, blob, putOptions);

        // 3. We may have executed some Streamlets or not. At this point, we need to
        // infer the next Swarmlet for the
        // request. We may need to forward it to another Swarmlet within the same Region
        // (Intra-Swarmlet Routing) due to
        // lack of hardware. Or we may need to just forward the request to the next
        // Region (e.g., EDGE, CLOUD), if any.
        // In routing is needed but no suitable Swarmlet is in metadata, a
        // NoSuitableSwarmletInRegionException will be
        // thrown. If nextSwarmletEndpoint is null, it means that there is no further
        // routing needed, so we can store
        // the data in the final destination (e.g., S3 bucket).
        String nextSwarmletEndpoint = null;
        try {
            nextSwarmletEndpoint = nextSwarmletRoutingEndpoint(policy, currentRegion, availableHardware);
        } catch (NoSuitableSwarmletInRegionException e) {
            // Cannot meet specified execution constraints in any Swarmlet, so throw
            // exception.
            throw new RuntimeException(e);
        }

        // 4. Routing the request to the next Nexus Swarmlet or just allowing the
        // S3Proxy to store the data in the final
        // destination (e.g., S3 bucket).
        if (nextSwarmletEndpoint != null) {
            forwardPutRequest(nextSwarmletEndpoint, containerName, blob).join();
            throw new ForwardedRequestException("Request forwarded to " + nextSwarmletEndpoint);
        } else {
            // 5. Reply to the client once the storage log storage completes, and forward
            // the operation to the next stage.
            return (putOptions == null) ? super.putBlob(containerName, blob)
                    : super.putBlob(containerName, blob, putOptions);
        }
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, null);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        // 1. Extract the system, scope, and stream for the incoming request and get the
        // policy (if any).
        Policy policy;
        try {
            policy = checkRequestAndRetrievePolicy(containerName, blobName);
        } catch (MalformedStreamStorageRequestException | NoPolicySetException e) {
            // Either the request is malformed or there are no policies, so just execute
            // operation against S3 endpoint.
            return super.getBlob(containerName, blobName, getOptions);
        } catch (Exception ex) {
            // Unexpected error, rethrow.
            throw new RuntimeException(ex);
        }

        // 2. For GETs, we need to check if we are the final Region, because the
        // processing goes backwards. In other
        // words, we get what is the next Swarmlet in the pipeline.
        Region currentRegion = this.metadataService.getNexusConfig().getRegion();
        Hardware availableHardware = this.metadataService.getNexusConfig().getHardware();
        String nextSwarmletEndpoint = null;
        try {
            nextSwarmletEndpoint = nextSwarmletRoutingEndpoint(policy, currentRegion, availableHardware);
        } catch (NoSuitableSwarmletInRegionException e) {
            // Cannot meet specified execution constraints in any Swarmlet, so throw
            // exception.
            throw new RuntimeException(e);
        }

        // 3. If we are the right Swarmlet in the final Region, we can proceed with the
        // GET to the actual storage. If
        // not, we need to forward the request to the next Swarmlet before doing or own
        // processing.
        Blob proxyBlob;
        if (nextSwarmletEndpoint != null) {
            proxyBlob = forwardGetRequest(nextSwarmletEndpoint, containerName, blobName).join();
        } else {
            proxyBlob = (getOptions == null) ? super.getBlob(containerName, blobName)
                    : super.getBlob(containerName, blobName, getOptions);
        }
        // 4. Try to execute the reverse Streamlets in our Region, if we are the right
        // Swarmlet instance.
        tryProcessGet(policy, currentRegion, availableHardware, containerName, proxyBlob, getOptions);
        return proxyBlob;
    }

    @Override
    public void removeBlob(String container, String name) {
        super.removeBlob(container, name);
    }

    @Override
    public void removeBlobs(String container, Iterable<String> iterable) {
        super.removeBlobs(container, iterable);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {
        return super.getBlobAccess(container, name);
    }

    @Override
    public void setBlobAccess(String container, String name, BlobAccess access) {
        super.setBlobAccess(container, name, access);
    }

    @Override
    public long countBlobs(String container) {
        return super.countBlobs(container);
    }

    @Override
    public long countBlobs(String container, ListContainerOptions options) {
        return super.countBlobs(container, options);
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String container, BlobMetadata blobMetadata, PutOptions options) {

        logger.info("--------Multipart upload initiated for container: {}--------", container);
        // Recording the start time for the multipart upload event
        multipartEventTime = System.nanoTime();
        return super.initiateMultipartUpload(container, blobMetadata, options);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        logger.info("///////Multipart upload aborted {}/////////", mpu);
        super.abortMultipartUpload(mpu);
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
        logger.info("--------Multipart upload successfully completed {}--------", parts);
        // Event timer recording
        StreamletsMetrics.MULTIPART_EVENT_TIMER.record(System.nanoTime() - multipartEventTime);
        return super.completeMultipartUpload(mpu, parts);
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {

        try {
            long startTime = System.nanoTime();
            payload = this.streamletsExecutor.interceptAndProcessMultipartUpload(mpu, partNumber, payload);
            logger.info("Part intercepted and successfully processed");

            StreamletsMetrics.MULTIPART_REQUEST_TIMER.record(System.nanoTime() - startTime);
        } catch (Exception e) {
            logger.error("Error while intercepting the current part", e);
        }

        return super.uploadMultipartPart(mpu, partNumber, payload);
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        return super.listMultipartUpload(mpu);
    }

    @Override
    public List<MultipartUpload> listMultipartUploads(String container) {
        return super.listMultipartUploads(container);
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return super.getMinimumMultipartPartSize();
    }

    @Override
    public long getMaximumMultipartPartSize() {
        return super.getMaximumMultipartPartSize();
    }

    @Override
    public int getMaximumNumberOfParts() {
        return super.getMaximumNumberOfParts();
    }

    @Override
    public void downloadBlob(String container, String name, File destination) {
        super.downloadBlob(container, name, destination);
    }

    @Override
    public void downloadBlob(String container, String name, File destination, ExecutorService executor) {
        super.downloadBlob(container, name, destination, executor);
    }

    @Override
    public InputStream streamBlob(String container, String name) {
        return super.streamBlob(container, name);
    }

    @Override
    public InputStream streamBlob(String container, String name, ExecutorService executor) {
        return super.streamBlob(container, name, executor);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    // end region

    // being private methods region

    private void tryProcessPut(Policy policy, Region currentRegion, Hardware availableHardware, String containerName,
            Blob blob, PutOptions putOptions) {
        if (policy.canSwarmletExecuteStreamlets(currentRegion, availableHardware)) {
            // We are in the right Swarmlet, process the request.
            doProcessPut(policy, containerName, blob, putOptions);
        }
    }

    private void tryProcessGet(Policy policy, Region currentRegion, Hardware availableHardware, String containerName,
            Blob blob, GetOptions getOptions) {
        if (policy.canSwarmletExecuteStreamlets(currentRegion, availableHardware)) {
            // We are in the right Swarmlet, process the request.
            doProcessGet(policy, containerName, blob, getOptions);
        }
    }

    private void doProcessPut(Policy policy, String containerName, Blob blob, PutOptions putOptions) {
        try {
            long startTime = System.nanoTime();
            this.streamletsExecutor.processRequest(policy, containerName, blob, true);
            logger.info("--------PUT request's blob successfully intercepted and processed--------");
            StreamletsMetrics.PUT_REQUEST_TIMER.record(System.nanoTime() - startTime);
        } catch (Exception e) {
            logger.error("Error while intercepting the PUT request", e);
        }
    }

    private void doProcessGet(Policy policy, String containerName, Blob proxyBlob, GetOptions getOptions) {
        try {
            long startTime = System.nanoTime();
            this.streamletsExecutor.processRequest(policy, containerName, proxyBlob, false);
            logger.info("--------GET request's blob successfully intercepted and processed--------");
            StreamletsMetrics.GET_REQUEST_TIMER.record(System.nanoTime() - startTime);
        } catch (Exception e) {
            logger.error("Error while intercepting the GET request", e);
        }
    }

    private Policy getPolicyForStream(String scopeName, String streamName) {
        try {
            // First, check if there is any policy for this specific stream.
            Policy policy = this.metadataService.getPolicyByStream(scopeName, streamName);
            // If there is a policy for this stream, return it. If not, look for any
            // scope-level policy.
            return (policy != null) ? policy : this.metadataService.getPolicyByScope(scopeName);
        } catch (Exception e) {
            logger.warn("Error while retrieving from the metadata service");
            throw new RuntimeException(e);
        }
    }

    /**
     * This method takes
     *
     * @param policy
     * @param currentRegion
     * @param availableHardware
     * @return
     */
    private String nextSwarmletRoutingEndpoint(Policy policy, Region currentRegion, Hardware availableHardware) {
        String nextSwarmletEndpoint = null;
        Optional<Hardware> requiredHardware;
        switch (checkSwarmletRoutingNeeded(policy, currentRegion, availableHardware)) {
        case INTER_REGION:
            Region nextRegionToForward = policy.getNextRegionToForward(currentRegion);
            // Check if there is any special hardware to run streamlets in the next region.
            requiredHardware = policy.getSpecialHardwareInRegion(nextRegionToForward);
            // Find the swarmlet endpoint for the next region.
            nextSwarmletEndpoint = this.metadataService.getSwarmletDescriptorByRegionAndHardware(nextRegionToForward,
                    requiredHardware.orElse(Hardware.NONE));
            if (nextSwarmletEndpoint == null) {
                // We tried to route this request to a suitable Swarmlet in the next Region, but
                // there is none.
                logger.error("No Swarmlets in the next Region {} that meet hardware requirements {}",
                        nextRegionToForward, requiredHardware);
                throw new NoSuitableSwarmletInRegionException(
                        "No Swarmlets in Region that meet hardware requirements.");
            }
            break;
        case INTRA_REGION:
            List<StreamletExecutionDescriptor> streamletsToExecuteInRegion = policy
                    .getStreamletsForRegion(currentRegion);
            // At this point, we are sure that there is some Streamlet in this Region
            // requiring special hardware.
            requiredHardware = streamletsToExecuteInRegion.stream()
                    .filter(s -> !s.getStreamlet().getHardware().equals(Hardware.NONE)) // Discard the NONE streamlet
                                                                                        // hardware
                    .filter(s -> !s.getStreamlet().getHardware().equals(availableHardware)) // Get the ones we cannot
                                                                                            // match
                    .map(s -> s.getStreamlet().getHardware()).findFirst();
            nextSwarmletEndpoint = this.metadataService.getSwarmletDescriptorByRegionAndHardware(currentRegion,
                    requiredHardware.get());
            if (nextSwarmletEndpoint == null) {
                // We tried to route this request to a suitable Swarmlet in this Region, but
                // there is none.
                logger.error("No Swarmlets in Region {} that meet hardware requirements {}", currentRegion,
                        availableHardware);
                throw new NoSuitableSwarmletInRegionException(
                        "No Swarmlets in Region that meet hardware requirements.");
            }
            break;
        default:
            // This Swarmlet can execute the policy in this region.
            logger.info("This is the terminal pipeline Region ({}), storing data to storage.", currentRegion);
        }
        return (nextSwarmletEndpoint == null || nextSwarmletEndpoint.endsWith("/")) ? nextSwarmletEndpoint
                : nextSwarmletEndpoint + "/";
    }

    /**
     * Checks whether this Swarmlet can execute the Streamlets for the specified
     * Region and Hardware requirements. If not, it outputs the right type of
     * routing needed to forward the request (inter or intra Swarmlet).
     *
     * @param policy Policy associated with the storage request of this stream.
     * @return Type of routing needed to forward this storage request.
     */
    private InterSwarmletRoutingType checkSwarmletRoutingNeeded(Policy policy, Region currentRegion,
            Hardware availableHardware) {
        // 1.Check if there is a next region or not
        if (policy.getNextRegionToForward(currentRegion) == null) {
            // This is the terminal point of the pipeline, store the data directly in the
            // bucket.
            return InterSwarmletRoutingType.NO_INTER_SWARMLET_ROUTING;
        }

        // 2. Check if this instance has been able to execute Streamlets for this
        // Region. In the affirmative case, it
        // means that we have to forward the request to the next Region. Otherwise, this
        // Swamrlet may not have the
        // necessary hardware for executing the Streamlets, so forward to another
        // Swarmlet in this Region if possible.
        boolean noStreamletsInRegion = policy.getStreamletsForRegion(currentRegion).isEmpty();
        boolean swarmletCanExecuteStreamlets = policy.canSwarmletExecuteStreamlets(currentRegion, availableHardware);
        return noStreamletsInRegion || swarmletCanExecuteStreamlets ? InterSwarmletRoutingType.INTER_REGION
                : InterSwarmletRoutingType.INTRA_REGION;
    }

    private Policy checkRequestAndRetrievePolicy(String containerName, Blob blob) {
        return checkRequestAndRetrievePolicy(containerName, blob.getMetadata().getName());
    }

    private Policy checkRequestAndRetrievePolicy(String containerName, String blobName) {
        String scope = StreamNameUtils.getScopeFromChunkName(blobName);
        String stream = StreamNameUtils.getStreamFromChunkName(blobName);
        if (scope == null || stream == null) {
            // Problem parsing the object path, skipping.
            logger.warn("Malformed object name being intercepted {}", blobName);
            throw new MalformedStreamStorageRequestException("Malformed object name being intercepted");
        }

        // 2. Check if there is any policy to apply to this storage operation.
        long startTime = System.nanoTime();
        Policy policy = getPolicyForStream(scope, stream);
        if (policy == null) {
            // If there is no policy, just forward the storage to the next swarmlet or final
            // destination.
            logger.warn("No policy set for scope/stream of object {}", blobName);
            throw new NoPolicySetException("No policy set for scope/stream of object");
        }
        StreamletsMetrics.POLICY_RETRIEVAL_TIMER.record(System.nanoTime() - startTime);
        return policy;
    }

    /**
     * Forward a GET request and retrieve the blob.
     *
     * @param targetServerUrl The endpoint to forward the GET request.
     * @param container       The container name.
     * @param blobName        The blob name to forward.
     * @return A CompletableFuture representing the operation and the retrieved
     *         blob.
     */
    private CompletableFuture<Blob> forwardGetRequest(String targetServerUrl, String container, String blobName) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Construct the target URL
                String curatedTargetURL = !targetServerUrl.startsWith(HTTP_URL_PREFIX)
                        ? HTTP_URL_PREFIX + targetServerUrl
                        : targetServerUrl;
                String targetUrl = curatedTargetURL + container + "/" + blobName;
                URL url = new URL(targetUrl);
                logger.info("Starting forward GET to {}.", targetUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");

                // Handle the response
                int responseCode = connection.getResponseCode();
                if (responseCode >= 200 && responseCode < 300) {
                    // Read the response body and construct a blob
                    InputStream inputStream = connection.getInputStream();
                    logger.info("Completed forward GET to {}, available bytes {}", targetUrl, inputStream.available());
                    Blob blob = super.blobBuilder(blobName).payload(inputStream).contentLength(100L)
                            .contentType(connection.getHeaderField(HttpHeaders.CONTENT_TYPE)).build();
                    // This metadata is needed by S3 proxy hanlder.
                    blob.getMetadata().setLastModified(new Date());
                    return blob;
                } else {
                    throw new RuntimeException("Failed to forward request, HTTP code: " + responseCode);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error while forwarding request", e);
            }
        });
    }

    /**
     * Forward PUT requests asynchronously.
     *
     * @param targetServerUrl Target server URL.
     * @param container       The container name.
     * @param blob            The blob to forward.
     * @return A CompletableFuture representing the operation.
     */
    private CompletableFuture<Void> forwardPutRequest(String targetServerUrl, String container, Blob blob) {
        return CompletableFuture.runAsync(() -> {
            try {
                // Construct the target URL
                String curatedTargetURL = !targetServerUrl.startsWith(HTTP_URL_PREFIX)
                        ? HTTP_URL_PREFIX + targetServerUrl
                        : targetServerUrl;
                String targetUrl = curatedTargetURL + container + "/" + blob.getMetadata().getName();
                logger.info("Starting forward PUT to {}.", targetUrl);
                URL url = new URL(targetUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("PUT");
                connection.setDoOutput(true);

                // Set headers from blob metadata
                blob.getMetadata().getUserMetadata().forEach(connection::setRequestProperty);
                connection.setRequestProperty(HttpHeaders.CONTENT_TYPE,
                        blob.getMetadata().getContentMetadata().getContentType());
                connection.setRequestProperty(HttpHeaders.CONTENT_LENGTH,
                        String.valueOf(blob.getPayload().getContentMetadata().getContentLength()));

                // Write the payload
                try (InputStream inputStream = blob.getPayload().openStream();
                        OutputStream outputStream = connection.getOutputStream()) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
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
        });
    }

    // end region
}
