package io.nexus.streamlets;

import com.google.common.net.HttpHeaders;

import io.nexus.shared.metrics.TimerMetric;
import io.nexus.streamlets.context.ContextManager;
import io.nexus.streamlets.context.RequestContext;
import io.nexus.streamlets.metadata.Hardware;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.StreamletExecutionDescriptor;
import io.nexus.streamlets.utils.FastPipedInputStream;
import io.nexus.streamlets.utils.FastPipedOutputStream;
import io.nexus.streamlets.utils.MultiPartUploadState;
import io.nexus.streamlets.utils.StreamNameUtils;
import io.pravega.common.concurrent.ExecutorServiceHelpers;

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
import org.jclouds.io.Payloads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * S3 Proxy middleware that intercepts storage requests and injects them into {@link StreamletsExecutor}.
 */
public class StreamletsInterceptor extends ForwardingBlobStore {

    private static final String HTTP_URL_PREFIX = "http://";
    final Logger logger = LoggerFactory.getLogger(StreamletsInterceptor.class);
    private final StreamletsExecutor streamletsExecutor;
    private final MetadataService metadataService;
    private final ConcurrentHashMap<String, MultiPartUploadState> multipartUploads;
    private final ScheduledExecutorService dataTransferExecutor;

    private enum InterSwarmletRoutingType {
        INTRA_REGION,
        INTER_REGION,
        NO_INTER_SWARMLET_ROUTING
    }

    public StreamletsInterceptor(BlobStore blobStore, MetadataService metadataService) {
        super(blobStore);
        this.metadataService = metadataService;
        this.streamletsExecutor = new StreamletsExecutor(metadataService);
        this.multipartUploads = new ConcurrentHashMap<>();
        this.dataTransferExecutor = ExecutorServiceHelpers.newScheduledThreadPool(40, "data-transfer-threadpool");
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
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
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
        logger.info("PUT request for {} / {}.", containerName, blob.getMetadata().getName());
        // 1. Extract the system, scope, and stream for the incoming request and get the policy (if any).
        Policy policy;
        StreamPartitionPojo streamPartition;
        try {
            policy = checkRequestAndRetrievePolicy(containerName, blob);
            streamPartition = StreamPartitionPojo.getStreamPartitionPojo(blob.getMetadata().getName(), policy.getSystem(), containerName);
        } catch (MalformedStreamStorageRequestException | NoPolicySetException e) {
            // Either the request is malformed or there are no policies, so just execute operation against S3 endpoint.
            logger.warn("Malformed request, forwarding without processing.", e);
            return super.putBlob(containerName, blob, putOptions);
        } catch (Exception ex) {
            // Unexpected error, rethrow.
            throw new RuntimeException(ex);
        }
        try {
            // 2. There is a policy set for the current stream. Let's be optimistic and assume we can perform the processing
            // of the Streamlets in this Region (if any) checking first that we have the right hardware available.
            Region currentRegion = this.metadataService.getNexusConfig().getRegion();
            Hardware availableHardware = this.metadataService.getNexusConfig().getHardware();
            // These streams connect with the output of the Streamlet pipeline, so we can perform forward async.
            InputStream streamletInput = blob.getPayload().openStream();
            FastPipedOutputStream streamletOutput = new FastPipedOutputStream();
            InputStream streamletOutputInputStream = new FastPipedInputStream(streamletOutput);
            CompletableFuture<Void> pipelineFuture = tryProcess(policy, currentRegion, availableHardware,
                    streamPartition, streamletInput, true, streamletOutput);
            // 3. We may have executed some Streamlets or not. At this point, we need to infer the next Swarmlet for the
            // request. We may need to forward it to another Swarmlet within the same Region (Intra-Swarmlet Routing) due to
            // lack of hardware. Or we may need to just forward the request to the next Region (e.g., EDGE, CLOUD), if any.
            // In routing is needed but no suitable Swarmlet is in metadata, a NoSuitableSwarmletInRegionException will be
            // thrown. If nextSwarmletEndpoint is null, it means that there is no further routing needed, so we can store
            // the data in the final destination (e.g., S3 bucket).
            String nextSwarmletEndpoint = nextSwarmletRoutingEndpoint(policy, currentRegion, availableHardware);
            if (nextSwarmletEndpoint != null) {
                // 4.1 Route the PUT request to the next Nexus Swarmlet.
                forwardPutRequest(nextSwarmletEndpoint, containerName, blob, streamletOutputInputStream).join();
                throw new ForwardedRequestException("Request forwarded to " + nextSwarmletEndpoint);
            } else {
                // 4.2 Store the PUT contents in S3 and reply to the client.
                Blob newBlob = this.blobBuilder(blob.getMetadata().getName())
                        .payload(Payloads.newInputStreamPayload(streamletOutputInputStream))
                        // TODO: Some S3-compatible backends require content length, which is problematic for streamlets
                        //  that change the length of the request when processed in streaming fashion.
                        .contentLength(blob.getPayload().getContentMetadata().getContentLength())
                        .contentType(blob.getPayload().getContentMetadata().getContentType())
                        .build();
                String eTag = (putOptions == null) ? super.putBlob(containerName, newBlob) :
                        super.putBlob(containerName, newBlob, putOptions);
                pipelineFuture.join();
                return eTag;
            }
        } catch (ForwardedRequestException e) {
            // Cannot meet specified execution constraints in any Swarmlet, so throw exception.
            throw e;
        } catch (Exception ex) {
            // Unknown problem, rethrow.
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, null);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        logger.info("GET request for {} / {}.", containerName, blobName);
        // 1. Extract the system, scope, and stream for the incoming request and get the policy (if any).
        Policy policy;
        StreamPartitionPojo streamPartition;
        try {
            policy = checkRequestAndRetrievePolicy(containerName, blobName);
            // TODO: Get the partition name correctly.
            streamPartition = StreamPartitionPojo.getStreamPartitionPojo(blobName, policy.getSystem(), containerName);
        } catch (MalformedStreamStorageRequestException | NoPolicySetException e) {
            // Either the request is malformed or there are no policies, so just execute operation against S3 endpoint.
            return super.getBlob(containerName, blobName, getOptions);
        } catch (Exception ex) {
            // Unexpected error, rethrow.
            throw new RuntimeException(ex);
        }

        try {
            // 2. For GETs, we need to check if we are the final Region, because the processing goes backwards. In other
            // words, we get what is the next Swarmlet in the pipeline.
            Region currentRegion = this.metadataService.getNexusConfig().getRegion();
            Hardware availableHardware = this.metadataService.getNexusConfig().getHardware();
            String nextSwarmletEndpoint = nextSwarmletRoutingEndpoint(policy, currentRegion, availableHardware);

            // 3. If we are the right Swarmlet in the final Region, we can proceed with the GET to the actual storage. If
            // not, we need to forward the request to the next Swarmlet before doing or own processing.
            InputStream streamletInput;
            FastPipedOutputStream streamletOutput = new FastPipedOutputStream();
            InputStream streamletOutputInputStream = new FastPipedInputStream(streamletOutput);
            FastPipedOutputStream streamletInputOutputStream = new FastPipedOutputStream();
            streamletInput = new FastPipedInputStream(streamletInputOutputStream);
            CompletableFuture<Void> getFuture = (nextSwarmletEndpoint != null) ?
                forwardGetRequest(nextSwarmletEndpoint, containerName, blobName, streamletInputOutputStream) :
                doAsyncGetRequest(containerName, blobName, getOptions, streamletInputOutputStream);
            // 4. Try to execute the reverse Streamlets in our Region, if we are the right Swarmlet instance.
            CompletableFuture<Void> transfersFuture = CompletableFuture.allOf(getFuture, tryProcess(policy, currentRegion,
                    availableHardware, streamPartition, streamletInput, false, streamletOutput))
                    .thenRun(() -> closeStreams(streamletInput, streamletOutputInputStream, streamletOutput));
            Blob resultBlob = super.blobBuilder(blobName)
                    .payload(streamletOutputInputStream)
                    .build();
            // This metadata is needed by S3 proxy handler.
            resultBlob.getMetadata().setLastModified(new Date());
            return resultBlob;
        } catch (NoSuitableSwarmletInRegionException e) {
            // Cannot meet specified execution constraints in any Swarmlet, so throw exception.
            logger.error("Cannot meet specified execution constraints in any Swarmlet {}", policy);
            throw new RuntimeException(e);
        } catch (Exception ex) {
            // Problem dealing with data streams.
            logger.error("Problem dealing with data streams.");
            throw new RuntimeException(ex);
        }
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
        this.logger.info("Multipart upload initiated for file {}", blobMetadata.getName());
        // Generate a unique upload ID.
        String uploadId = UUID.randomUUID().toString();
        // Create a state tracker for this multipart upload.
        MultiPartUploadState uploadState = new MultiPartUploadState(container, blobMetadata, options, uploadId);
        this.multipartUploads.put(uploadId, uploadState);
        // Return a valid MultipartUpload object.
        return MultipartUpload.create(container, blobMetadata.getName(), uploadId, blobMetadata, options);
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
        logger.info("Uploading part {} for multi-part upload {}.", partNumber, mpu.id());
        MultiPartUploadState uploadState = this.multipartUploads.get(mpu.id());
        if (uploadState == null || !uploadState.getUploadId().equals(mpu.id())) {
            throw new IllegalStateException("Upload ID not found or not matching");
        }
        // Buffer parts of the upload as input data to the created PUT request payload.
        long transferredBytes = uploadState.uploadPart(partNumber, payload);
        logger.info("Completed part in multipart upload {} with {} bytes.", mpu.id(), transferredBytes);
        // Return the result to the client for this part.
        return MultipartPart.create(partNumber, transferredBytes, UUID.randomUUID().toString(), new Date());
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        MultiPartUploadState uploadState = this.multipartUploads.get(mpu.id());
        if (uploadState == null || !uploadState.getUploadId().equals(mpu.id())) {
            throw new IllegalStateException("Upload ID not found or not matching");
        }
        // Cancel the PUT request and release the buffers.
        uploadState.abortUpload();
        this.multipartUploads.remove(mpu.id());
        logger.info("Multipart upload aborted {} ({})", mpu.blobName(), mpu.id());
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
        MultiPartUploadState uploadState = this.multipartUploads.get(mpu.id());
        if (uploadState == null || !uploadState.getUploadId().equals(mpu.id())) {
            throw new IllegalStateException("Upload ID not found or not matching");
        }
        // Create the PUT request from the completed multipart upload.
        initiatePutRequestFromMultipartUpload(uploadState);
        // Tansfer the data from all the parts to the output stream related to the PUT request.
        uploadState.transferMultiPartContentsToPutRequest();
        // Wait until the PUT request completes to make sure we can reply to the client.
        uploadState.completeUpload();
        this.multipartUploads.remove(mpu.id());
        logger.info("Multipart upload complete {} ({}).", mpu.blobName(), mpu.id());
        return uploadState.getUploadId();
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        MultiPartUploadState state = this.multipartUploads.get(mpu.id());
        if (state == null) {
            return Collections.emptyList(); // No active upload found
        }
        return state.listParts();
    }

    @Override
    public List<MultipartUpload> listMultipartUploads(String container) {
        return this.multipartUploads.values().stream()
                .filter(mpu -> mpu.getContainer().equals(container))
                .map(state -> MultipartUpload.create(state.getContainer(), state.getBlobMetadata().getName(),
                        state.getUploadId(), state.getBlobMetadata(), state.getOptions()))
                .collect(Collectors.toList());
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return 5 * 1024 * 1024; // 5MB, standard for S3
    }

    @Override
    public long getMaximumMultipartPartSize() {
        return 5L * 1024 * 1024 * 1024; // 5GB per part, per S3 specs
    }

    @Override
    public int getMaximumNumberOfParts() {
        return 10000; // S3's hard limit
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

    private CompletableFuture<Void> tryProcess(Policy policy, Region currentRegion, Hardware availableHardware,
                                               StreamPartitionPojo streamPartition, InputStream streamletInput, boolean isPut,
                                               OutputStream streamletResult) {
        if (policy.canSwarmletExecuteStreamlets(currentRegion, availableHardware)) {
            // We are in the right Swarmlet, process the request.
            return doProcess(policy, streamPartition, streamletInput, isPut, streamletResult);
        }
        return null;
    }

    private CompletableFuture<Void> doProcess(Policy policy, StreamPartitionPojo streamPartition, InputStream streamletInput,
                                              boolean isPut, OutputStream streamletResult) {
        try {
            long startTime = System.nanoTime();
            // Instantiate a context for the pipeline
            final ContextManager contextManager = ContextManager.getInstance();
            RequestContext streamletContext = contextManager.createRequestStreamletContext(logger, policy);
            return this.streamletsExecutor.processRequest(policy, streamPartition, streamletInput, isPut, streamletContext, streamletResult)
                    .thenRun(() -> { // Record processing time metrics after once processing completes.
                        TimerMetric timer = isPut ? StreamletsMetrics.PUT_REQUEST_TIMER : StreamletsMetrics.GET_REQUEST_TIMER;
                        timer.record(System.nanoTime() - startTime);
                    });
        } catch (Exception e) {
            logger.error("Error while intercepting the Request", e);
            throw new RuntimeException(e);
        }
    }

    private Policy getPolicyForStream(String scopeName, String streamName) {
        try {
            // First, check if there is any policy for this specific stream.
            Policy policy = this.metadataService.getPolicyByStream(scopeName, streamName);
            // If there is a policy for this stream, return it. If not, look for any scope-level policy.
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
                    // We tried to route this request to a suitable Swarmlet in the next Region, but there is none.
                    logger.error("No Swarmlets in the next Region {} that meet hardware requirements {}",
                            nextRegionToForward, requiredHardware);
                    throw new NoSuitableSwarmletInRegionException("No Swarmlets in Region that meet hardware requirements.");
                }
                break;
            case INTRA_REGION:
                List<StreamletExecutionDescriptor> streamletsToExecuteInRegion = policy.getStreamletsForRegion(currentRegion);
                // At this point, we are sure that there is some Streamlet in this Region requiring special hardware.
                requiredHardware = streamletsToExecuteInRegion.stream()
                        .filter(s -> !s.getStreamlet().getHardware().equals(Hardware.NONE)) // Discard the NONE streamlet hardware
                        .filter(s -> !s.getStreamlet().getHardware().equals(availableHardware)) // Get the ones we cannot match
                        .map(s -> s.getStreamlet().getHardware())
                        .findFirst();
                nextSwarmletEndpoint = this.metadataService.getSwarmletDescriptorByRegionAndHardware(currentRegion, requiredHardware.get());
                if (nextSwarmletEndpoint == null) {
                    // We tried to route this request to a suitable Swarmlet in this Region, but there is none.
                    logger.error("No Swarmlets in Region {} that meet hardware requirements {}", currentRegion, availableHardware);
                    throw new NoSuitableSwarmletInRegionException("No Swarmlets in Region that meet hardware requirements.");
                }
                break;
            default:
                // This Swarmlet can execute the policy in this region.
                logger.info("This is the terminal pipeline Region ({}), storing data to storage.", currentRegion);
        }
        return (nextSwarmletEndpoint == null || nextSwarmletEndpoint.endsWith("/")) ?
                nextSwarmletEndpoint : nextSwarmletEndpoint + "/";
    }

    /**
     * Checks whether this Swarmlet can execute the Streamlets for the specified Region and Hardware requirements. If
     * not, it outputs the right type of routing needed to forward the request (inter or intra Swarmlet).
     *
     * @param policy Policy associated with the storage request of this stream.
     * @return Type of routing needed to forward this storage request.
     */
    private InterSwarmletRoutingType checkSwarmletRoutingNeeded(Policy policy, Region currentRegion, Hardware availableHardware) {
        // 1.Check if there is a next region or not
        if (policy.getNextRegionToForward(currentRegion) == null) {
            // This is the terminal point of the pipeline, store the data directly in the bucket.
            return InterSwarmletRoutingType.NO_INTER_SWARMLET_ROUTING;
        }

        // 2. Check if this instance has been able to execute Streamlets for this Region. In the affirmative case, it
        // means that we have to forward the request to the next Region. Otherwise, this Swamrlet may not have the
        // necessary hardware for executing the Streamlets, so forward to another Swarmlet in this Region if possible.
        boolean noStreamletsInRegion = policy.getStreamletsForRegion(currentRegion).isEmpty();
        boolean swarmletCanExecuteStreamlets = policy.canSwarmletExecuteStreamlets(currentRegion, availableHardware);
        return noStreamletsInRegion || swarmletCanExecuteStreamlets ?
                InterSwarmletRoutingType.INTER_REGION : InterSwarmletRoutingType.INTRA_REGION;
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
            // If there is no policy, just forward the storage to the next swarmlet or final destination.
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
     * @param container The container name.
     * @param blobName  The blob name to forward.
     * @return A CompletableFuture representing the operation and the retrieved blob.
     */
    private CompletableFuture<Void> forwardGetRequest(String targetServerUrl, String container, String blobName, OutputStream getContents) {
        return CompletableFuture.runAsync(() -> {
        try {
            // Construct the target URL
            String curatedTargetURL = !targetServerUrl.startsWith(HTTP_URL_PREFIX) ?
                    HTTP_URL_PREFIX + targetServerUrl : targetServerUrl;
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
                long transferredBytes = inputStream.transferTo(getContents);
                logger.info("Completed forward GET to {}, available bytes {}", targetUrl, transferredBytes);
            } else {
                throw new RuntimeException("Failed to forward request, HTTP code: " + responseCode);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while forwarding request", e);
        }

        }, this.dataTransferExecutor);
    }

    /**
     * Forward PUT requests asynchronously.
     *
     * @param targetServerUrl    Target server URL.
     * @param container The container name.
     * @param blob      The blob to forward.
     * @return A CompletableFuture representing the operation.
     */
    private CompletableFuture<Void> forwardPutRequest(String targetServerUrl, String container, Blob blob,
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

    private void initiatePutRequestFromMultipartUpload(MultiPartUploadState uploadState) {
        // Check if this is the first part for this upload.
        if (uploadState.isUploadInitialized().compareAndSet(false, true)) {
            logger.info("Creating blob metadata {}.", uploadState.getUploadId());
            // Create a blob with the metadata from the original request
            Blob blob = this.blobBuilder(uploadState.getBlobMetadata().getName())
                    .payload(Payloads.newInputStreamPayload(uploadState.getInputStream()))
                    .contentLength(uploadState.getMultipartUploadSize())
                    .contentType(uploadState.getBlobMetadata().getContentMetadata().getContentType())
                    .build();

            // The first time that we upload a part, in addition to transferring the data, we have to initiate the PUT.
            uploadState.setPutRequest(CompletableFuture.runAsync(() ->
                            this.putBlob(uploadState.getContainer(), blob), this.dataTransferExecutor)
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

    private CompletableFuture<Void> doAsyncGetRequest(String containerName, String blobName, GetOptions getOptions,
                                                      OutputStream streamletInputOutputStream) {
        return CompletableFuture.runAsync(() -> {
            Blob proxyBlob = (getOptions == null) ? super.getBlob(containerName, blobName) :
                    super.getBlob(containerName, blobName, getOptions);
            try {
                int readBytes;
                byte[] content = new byte[16 * 1024];
                InputStream is = proxyBlob.getPayload().openStream();
                while ((readBytes = is.read(content)) != -1) {
                    streamletInputOutputStream.write(content, 0, readBytes);
                }
                is.close();
                streamletInputOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, this.dataTransferExecutor);
    }

    public static void closeStreams(AutoCloseable... streams) {
        Exception firstException = null;
        for (AutoCloseable stream : streams) {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception e) {
                    if (firstException == null) {
                        firstException = e;
                    } else {
                        firstException.addSuppressed(e);
                    }
                }
            }
        }
        if (firstException != null) {
            throw new RuntimeException("Error closing streams", firstException);
        }
    }

    // end region
}
