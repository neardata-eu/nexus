package io.nexus.streamlets;

import io.nexus.shared.metrics.GaugeMetric;
import io.nexus.shared.metrics.TimerMetric;
import io.nexus.streamlets.context.ContextManager;
import io.nexus.streamlets.context.RequestContext;

import io.nexus.streamlets.metadata.Hardware;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.S3StorageConfig;
import io.nexus.streamlets.metadata.StreamletExecutionDescriptor;
import io.nexus.streamlets.utils.CachedS3Client;
import io.nexus.streamlets.utils.FastPipedInputStream;
import io.nexus.streamlets.utils.FastPipedOutputStream;
import io.nexus.streamlets.utils.MultiPartUploadState;
import io.nexus.streamlets.utils.ObjectTagsUtils;
import io.nexus.streamlets.utils.RequestManager;
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * S3 Proxy middleware that intercepts storage requests and injects them into {@link StreamletsExecutor}.
 */
public class StreamletsInterceptor extends ForwardingBlobStore implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(StreamletsInterceptor.class);
    private final StreamletsExecutor streamletsExecutor;
    private final MetadataService metadataService;
    private final ConcurrentHashMap<String, MultiPartUploadState> multipartUploads;
    private final ContextManager contextManager;
    private final RequestManager requestManager;
    private final CachedS3Client cachedS3Client;
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
        this.contextManager = ContextManager.getInstance();
        this.cachedS3Client = new CachedS3Client();
        this.dataTransferExecutor = ExecutorServiceHelpers.newScheduledThreadPool(40, "data-transfer-threadpool");
        this.requestManager = new RequestManager(this, dataTransferExecutor);
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
        long startTime = System.nanoTime();
        if (StreamNameUtils.getSystemFromChunk(blob.getMetadata().getName()) == null) {
            logger.info("Skipping PUT interception for non-log/ledger blob: {}", blob.getMetadata().getName());
            return (putOptions == null) ? super.putBlob(containerName, blob) : super.putBlob(containerName, blob, putOptions);
        }
        logger.info("PUT request for {} / {}.", containerName, blob.getMetadata().getName());
        // 1. Extract the system, scope, and stream for the incoming request and get the policy (if any).
        Policy policy;
        StreamPartition streamPartition;
        try {
            policy = checkRequestAndRetrievePolicy(containerName, blob);
            streamPartition = StreamPartition.getStreamPartitionPojo(blob.getMetadata().getName(), policy.getSystem(), containerName);
        } catch (MalformedStreamStorageRequestException | NoPolicySetException e) {
            // Either the request is malformed or there are no policies, so just execute operation against S3 endpoint.
            logger.warn("Malformed request or request without policy, forwarding without processing.");
            return (putOptions == null) ? super.putBlob(containerName, blob) : super.putBlob(containerName, blob, putOptions);
        } catch (Exception ex) {
            // Unexpected error, rethrow.
            logger.error("Unexpected error while getting stream policy.", ex);
            throw new RuntimeException(ex);
        }
        try {
            // 2. There is a policy set for the current stream. Let's be optimistic and assume we can perform the processing
            // of the Streamlets in this Region (if any) checking first that we have the right hardware available.
            Region currentRegion = this.metadataService.getNexusConfig().getRegion();
            Hardware availableHardware = this.metadataService.getNexusConfig().getHardware();
            // Instantiate a context for the pipeline
            List<S3StorageConfig> s3StorageConfigs = this.metadataService.getS3ConfigsForPolicy(policy);
            RequestContext streamletContext = this.contextManager.createRequestStreamletContext(logger, policy,
                    streamPartition, s3StorageConfigs, this.cachedS3Client);
            streamletContext.addTransformerStreamletsToMetadata(currentRegion);
            // These streams connect with the output of the Streamlet pipeline, so we can perform forward async.
            InputStream streamletInput = blob.getPayload().openStream();
            FastPipedOutputStream streamletOutput = new FastPipedOutputStream();
            InputStream streamletOutputInputStream = new FastPipedInputStream(streamletOutput);
            CompletableFuture<Long> pipelineFuture = tryProcess(policy, currentRegion, availableHardware,
                    streamPartition, streamletInput, true, streamletOutput, streamletContext);
            // 3. We may have executed some Streamlets or not. At this point, we need to infer the next Swarmlet for the
            // request. We may need to forward it to another Swarmlet within the same Region (Intra-Swarmlet Routing) due to
            // lack of hardware. Or we may need to just forward the request to the next Region (e.g., EDGE, CLOUD), if any.
            // In routing is needed but no suitable Swarmlet is in metadata, a NoSuitableSwarmletInRegionException will be
            // thrown. If nextSwarmletEndpoint is null, it means that there is no further routing needed, so we can store
            // the data in the final destination (e.g., S3 bucket).
            String nextSwarmletEndpoint = nextSwarmletRoutingEndpoint(policy, currentRegion, availableHardware);
            // We consider that the interception time up to this point, next phase is mainly transfer and processing.
            StreamletsMetrics.PUT_REQUEST_INTERCEPTION_DURATION_TIMER.record(System.nanoTime() - startTime);
            if (nextSwarmletEndpoint != null) {
                // 4.1 Route the PUT request to the next Nexus Swarmlet and update the metadata in storage.
                this.requestManager.forwardPutRequest(nextSwarmletEndpoint, containerName, blob, streamletOutputInputStream)
                        .thenCompose(v -> this.requestManager.updateMetadataAsync(containerName,
                                blob.getMetadata().getName(), streamletContext)).join();
                throw new ForwardedRequestException("Request forwarded to " + nextSwarmletEndpoint);
            } else {
                // 4.2 Store the PUT contents in S3 and reply to the client.
                startTime = System.nanoTime();
                long contentLength = blob.getPayload().getContentMetadata().getContentLength();
                String eTag;
                // If there is a data routing streamlet, we interrupt the flow as the Streamlet is in charge of storing 
                // data in the right storage according to its logic.
                if (policy.hasDataRoutingStreamlet(currentRegion)) {
                    pipelineFuture.join();
                    throw new ForwardedRequestException("PUT routed to an alternative storage.");
                } else {
                    // Normal PUT against default storage.
                    Blob newBlob = buildPutBlob(blob, streamletOutputInputStream, contentLength);
                    eTag = (putOptions == null) ? super.putBlob(containerName, newBlob) :
                            super.putBlob(containerName, newBlob, putOptions);
                    // If there is user metadata, store it as tags in the object after the object is already stored.
                    pipelineFuture.join();
                    this.requestManager.updateMetadataAsync(containerName, blob.getMetadata().getName(), streamletContext);
                }
                // Record metrics.
                recordPutMetrics(startTime, contentLength);
                return eTag;
            }
        } catch (ForwardedRequestException e) {
            // Cannot meet specified execution constraints in any Swarmlet, so throw exception.
            throw e;
        } catch (Exception ex) {
            // Unknown problem, rethrow.
            logger.error("Unexpected exception in PUT interception.", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, null);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        long startTime = System.nanoTime();
        if (StreamNameUtils.getSystemFromChunk(blobName) == null) {
            logger.info("Skipping GET interception for non-log/ledger blob: {}", blobName);
            return super.getBlob(containerName, blobName, getOptions);
        }

        logger.info("GET request for {} / {}.", containerName, blobName);
        // 1. Extract the system, scope, and stream for the incoming request and get the policy (if any).
        Policy policy;
        Region currentRegion = this.metadataService.getNexusConfig().getRegion();
        Hardware availableHardware = this.metadataService.getNexusConfig().getHardware();
        try {
            policy = checkRequestAndRetrievePolicy(containerName, blobName);
        } catch (MalformedStreamStorageRequestException mssre) {
            logger.warn("Malformed GET request {}, executing against S3 storage.", blobName);
            // If the request is malformed execute operation against S3 endpoint.
            return (getOptions == null) ? super.getBlob(containerName, blobName) : super.getBlob(containerName, blobName, getOptions);
        } catch (NoPolicySetException npse) {
            // No policy in metadata, but we need to check for transformer Streamlets in this object.
            Blob headersBlob = (getOptions == null) ? super.getBlob(containerName, blobName) : super.getBlob(containerName, blobName, getOptions);
            if (headersBlob == null) {
                // This may happen in some rare concurrency situations. Report and return.
                logger.error("Null blob in GET request {} / {}, just returning", containerName, blobName);
                return headersBlob; 
            }
            List<String> transformerStreamlets = ObjectTagsUtils.getTransformerStreamletsFromRequest(
                    headersBlob.getAllHeaders(), currentRegion);
            if (!transformerStreamlets.isEmpty()) {
                logger.info("No policy set for {} / {}, but the object was processed by transformer Streamlets [{}].",
                        containerName, blobName, String.join(",", transformerStreamlets));
                policy = Policy.createMockPolicyForLegacyTransformerStreamlets(transformerStreamlets, currentRegion, this.metadataService);
            } else {
                // Plain object with no Streamlet transformations, so just execute operation against S3 endpoint.
                logger.info("No policy set for {} / {} and no transformer streamlets either", containerName, blobName);
                return headersBlob;
            }
        } catch (Exception ex) {
            // Unexpected error, rethrow.
            throw new RuntimeException(ex);
        }
        StreamPartition streamPartition = StreamPartition.getStreamPartitionPojo(blobName, policy.getSystem(), containerName);
        try {
            // 2. For GETs, we need to check if we are the final Region, because the processing goes backwards. In other
            // words, we get what is the next Swarmlet in the pipeline. For mock policies we do not apply routing.
            String nextSwarmletEndpoint = policy.isMock() ? null :
                    nextSwarmletRoutingEndpoint(policy, currentRegion, availableHardware);
            // 3. If we are the right Swarmlet in the final Region, we can proceed with the GET to the actual storage. If
            // not, we need to forward the request to the next Swarmlet before doing or own processing.
            InputStream streamletInput;
            FastPipedOutputStream streamletOutput = new FastPipedOutputStream();
            InputStream streamletOutputInputStream = new FastPipedInputStream(streamletOutput);
            FastPipedOutputStream streamletInputOutputStream = new FastPipedOutputStream();
            streamletInput = new FastPipedInputStream(streamletInputOutputStream);
            RequestContext streamletContext = this.contextManager.createRequestStreamletContext(logger, policy,
                    streamPartition, Collections.emptyList(), this.cachedS3Client);
            // We consider that the interception time up to this point, next phase is mainly transfer and processing.
            StreamletsMetrics.GET_REQUEST_INTERCEPTION_DURATION_TIMER.record(System.nanoTime() - startTime);
            // 4. Start the data transfer asynchronously depending on the pipeline definition. We first check if there
            // is a DataSourceStreamlet in this region before issuing the actual GET.
            boolean anyStreamletToExecute = policy.anyStreamletToRun(currentRegion, availableHardware, false);
            FastPipedOutputStream actualOutputStream = anyStreamletToExecute ? streamletInputOutputStream : streamletOutput;
            CompletableFuture<Void> getFuture = this.streamletsExecutor.getPreGetTransferFuture(policy, currentRegion,
                     // Transfer data to the pipeline only if there are other Streamlets
                     streamPartition, streamletContext, actualOutputStream);
            if (getFuture == null) {
                // 4.1 If data needs to be requested externally, decide the right type (i.e., forward or GET to storage).
                getFuture = (nextSwarmletEndpoint != null) ?
                        this.requestManager.forwardGetRequest(nextSwarmletEndpoint, containerName, blobName, actualOutputStream, streamletContext) :
                        this.requestManager.doAsyncGetRequest(containerName, blobName, getOptions, actualOutputStream, streamletContext);
            }
            Blob resultBlob = buildGetBlob(blobName, streamletOutputInputStream, streamletContext);
            // 5. Try to execute the reverse Streamlets in our Region, if we are the right Swarmlet instance.
            CompletableFuture<Void> pipelineFuture = CompletableFuture.allOf(getFuture, tryProcess(policy, currentRegion, availableHardware,
                            streamPartition, streamletInput, false, streamletOutput, streamletContext))
                    .handle(handleStreamletExceptions(streamletOutputInputStream, streamletInput, actualOutputStream));
            return resultBlob;
        } catch (NoSuitableSwarmletInRegionException e) {
            // Cannot meet specified execution constraints in any Swarmlet, so throw exception.
            logger.error("Cannot meet specified execution constraints in any Swarmlet {}", policy);
            throw new RuntimeException(e);
        } catch (Exception ex) {
            // Problem dealing with data streams.
            logger.error("Problem dealing with data streams.", ex);
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
        long startTime = System.nanoTime();
        if (StreamNameUtils.getSystemFromChunk(blobMetadata.getName()) == null){
            logger.info("Skipping multipart upload interception for non-log/ledger blob: {}", blobMetadata.getName());
            return super.initiateMultipartUpload(container, blobMetadata, options);
        }

        this.logger.info("Multipart upload initiated for file {}", blobMetadata.getName());
        // Generate a unique upload ID.
        String uploadId = UUID.randomUUID().toString();
        // Create a state tracker for this multipart upload.
        MultiPartUploadState uploadState = new MultiPartUploadState(container, blobMetadata, options, uploadId);
        this.multipartUploads.put(uploadId, uploadState);
        // Return a valid MultipartUpload object.
        MultipartUpload multipartUpload = MultipartUpload.create(container, blobMetadata.getName(), uploadId, blobMetadata, options);
        StreamletsMetrics.INITIATE_MULTIPART_REQUEST_INTERCEPTION_DURATION_TIMER.record(System.nanoTime() - startTime);
        return multipartUpload;
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
        long startTime = System.nanoTime();
        if (StreamNameUtils.getSystemFromChunk(mpu.blobName()) == null){
            logger.info("Continuing multipart interception skips...");
            return super.uploadMultipartPart(mpu, partNumber,payload);
        }

        logger.info("Uploading part {} for multi-part upload {}.", partNumber, mpu.id());
        MultiPartUploadState uploadState = this.multipartUploads.get(mpu.id());
        if (uploadState == null || !uploadState.getUploadId().equals(mpu.id())) {
            throw new IllegalStateException("Upload ID not found or not matching");
        }
        // Buffer parts of the upload as input data to the created PUT request payload.
        long transferredBytes = uploadState.uploadPart(partNumber, payload);
        logger.info("Completed part in multipart upload {} with {} bytes.", mpu.id(), transferredBytes);
        // Return the result to the client for this part.
        MultipartPart multipartPart = MultipartPart.create(partNumber, transferredBytes, UUID.randomUUID().toString(), new Date());
        // Record metrics.
        recordMultipartUploadMetrics(startTime, transferredBytes);
        return multipartPart;
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        long startTime = System.nanoTime();
        if (StreamNameUtils.getSystemFromChunk(mpu.blobName()) == null){
            logger.info("Aborting multipart interception for non-log/ledger blob: {}", mpu.blobName());
            super.abortMultipartUpload(mpu);
            return;
        }

        MultiPartUploadState uploadState = this.multipartUploads.get(mpu.id());
        if (uploadState == null || !uploadState.getUploadId().equals(mpu.id())) {
            throw new IllegalStateException("Upload ID not found or not matching");
        }
        // Cancel the PUT request and release the buffers.
        uploadState.abortUpload();
        this.multipartUploads.remove(mpu.id());
        logger.info("Multipart upload aborted {} ({})", mpu.blobName(), mpu.id());
        StreamletsMetrics.ABORT_MULTIPART_REQUEST_INTERCEPTION_DURATION_TIMER.record(System.nanoTime() - startTime);
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
        long startTime = System.nanoTime();
        if (StreamNameUtils.getSystemFromChunk(mpu.blobName()) == null) {
            logger.info("Completely skipped multipart upload interception for non-log/ledger blob: {}", mpu.blobName());
            return super.completeMultipartUpload(mpu, parts);
        }

        MultiPartUploadState uploadState = this.multipartUploads.get(mpu.id());
        if (uploadState == null || !uploadState.getUploadId().equals(mpu.id())) {
            throw new IllegalStateException("Upload ID not found or not matching");
        }
        // Create the PUT request from the completed multipart upload.
        this.requestManager.initiatePutRequestFromMultipartUpload(uploadState);
        // Transfer the data from all the parts to the output stream related to the PUT request.
        long transferredBytes = uploadState.transferMultiPartContentsToPutRequest();
        // Wait until the PUT request completes to make sure we can reply to the client.
        uploadState.completeUpload();
        this.multipartUploads.remove(mpu.id());
        logger.info("Multipart upload complete {} ({}).", mpu.blobName(), mpu.id());
        // Record metrics.
        double operationTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
        StreamletsMetrics.MULTIPART_UPLOAD_COMPLETE_OPERATIONS_COUNTER.incrementCounter();
        double transferSizeMB = (transferredBytes / (1024.0 * 1024.0));
        StreamletsMetrics.MULTIPART_UPLOAD_COMPLETE_SIZE_GAUGE.record(transferSizeMB);
        double transferSpeedMBps = transferSizeMB / operationTimeSeconds;
        StreamletsMetrics.MULTIPART_UPLOAD_COMPLETE_THROUGHPUT_GAUGE.record(transferSpeedMBps);
        return uploadState.getUploadId();
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        if (StreamNameUtils.getSystemFromChunk(mpu.blobName()) == null ) {
            logger.info("Skipping interception for non-log/ledger blob: {}", mpu.blobName());
            return super.listMultipartUpload(mpu);
        }

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

    @Override
    public void close() throws IOException {
        this.requestManager.close();
        this.cachedS3Client.close();
        ExecutorServiceHelpers.shutdown(this.dataTransferExecutor);
    }

    // end region

    // being private methods region

    private CompletableFuture<Long> tryProcess(Policy policy, Region currentRegion, Hardware availableHardware,
                                               StreamPartition streamPartition, InputStream streamletInput, boolean isPut,
                                               OutputStream streamletResult, RequestContext streamletContext) {
        if (policy.anyStreamletToRun(currentRegion, availableHardware, isPut)) {
            // We are in the right Swarmlet, process the request.
            return doProcess(policy, streamPartition, streamletInput, isPut, streamletResult, streamletContext);
        }
        logger.info("Trying to process but no Streamlets to execute in {} request for {}.", isPut ? "PUT" : "GET", streamPartition);
        return CompletableFuture.supplyAsync(() -> directIOTransfer(streamletInput, streamletResult));
    }

    private CompletableFuture<Long> doProcess(Policy policy, StreamPartition streamPartition, InputStream streamletInput,
                                              boolean isPut, OutputStream streamletResult, RequestContext streamletContext) {
        try {
            long startTime = System.nanoTime();
            return this.streamletsExecutor.processRequest(policy, streamPartition, streamletInput, isPut, streamletContext, streamletResult)
                    .thenApply(processedBytes -> {
                        // Record processing time metrics after once processing completes.
                        TimerMetric timer = isPut ? StreamletsMetrics.PUT_STREAMLET_PIPELINE_EXECUTION_LATENCY_TIMER :
                                StreamletsMetrics.GET_STREAMLET_PIPELINE_EXECUTION_LATENCY_TIMER;
                        timer.record(System.nanoTime() - startTime);
                        GaugeMetric gauge = isPut ? StreamletsMetrics.PUT_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT_GAUGE :
                                StreamletsMetrics.GET_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT_GAUGE;
                        gauge.record(GaugeMetric.getMBps(System.nanoTime() - startTime, processedBytes));
                        return processedBytes;
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
     * Determines the next Swarmlet routing endpoint based on the given policy, current region, and available hardware.
     * This method decides whether to forward the request to a Swarmlet in a different region (inter-region routing)
     * or within the same region (intra-region routing). If no suitable Swarmlet is found, an exception is thrown.
     *
     * @param policy           The routing policy defining Streamlet pipeline, regions, and hardware requirements.
     * @param currentRegion    The current region where the request is being processed.
     * @param availableHardware The hardware available in the current region for execution.
     * @return The next Swarmlet endpoint URL for routing, ensuring it ends with a trailing slash.
     * @throws NoSuitableSwarmletInRegionException If no Swarmlet meets the required hardware constraints in the selected region.
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
        // means that we have to forward the request to the next Region. Otherwise, this Swarmlet may not have the
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

    private <T> BiFunction<T, Throwable, T> handleStreamletExceptions(InputStream streamletOutputInputStream,
                                                                        InputStream streamletInput,
                                                                        FastPipedOutputStream streamletOutput) {
        return (v, ex) -> {
            if (ex != null) {
                try {
                    streamletOutputInputStream.close();
                    logger.error("Exception thrown during pipeline execution, closing stream.", ex);
                } catch (IOException e) {
                    logger.error("Exception while closing stream after pipeline execution error.", e);
                    throw new RuntimeException(e);
                }
            }
            closeStreams(streamletInput, streamletOutputInputStream, streamletOutput);
            return null;
        };
    }

    private static void closeStreams(AutoCloseable... streams) {
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

    private Blob buildGetBlob(String blobName, InputStream streamletOutputInputStream, RequestContext streamletContext) {
        Blob newBlob = super.blobBuilder(blobName)
                .payload(streamletOutputInputStream)
                .userMetadata(streamletContext.getUserMetadataCopy())
                .build();
        newBlob.getMetadata().setLastModified(new Date()); // This metadata is needed by S3 proxy handler.
        return newBlob;
    }

    private Blob buildPutBlob(Blob blob, InputStream streamletOutputInputStream, long contentLength) {
        return this.blobBuilder(blob.getMetadata().getName())
                .payload(Payloads.newInputStreamPayload(streamletOutputInputStream))
                // TODO: Some S3-compatible backends require content length, which is problematic for streamlets
                //  that change the length of the request when processed in streaming fashion.
                .contentLength(contentLength)
                .contentType(blob.getPayload().getContentMetadata().getContentType())
                .build();
    }

    private static long directIOTransfer(InputStream streamletInput, OutputStream streamletResult) {
        try {
            long transferredBytes =  streamletInput.transferTo(streamletResult);
            streamletInput.close();
            streamletResult.close();
            return transferredBytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void recordPutMetrics(long startTime, long contentLength) {
        double operationTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
        StreamletsMetrics.PUT_REQUEST_STORAGE_OPERATIONS_COUNTER.incrementCounter();
        double transferSizeMB = (contentLength / (1024.0 * 1024.0));
        StreamletsMetrics.PUT_REQUEST_STORAGE_SIZE_GAUGE.record(transferSizeMB);
        double transferSpeedMBps = transferSizeMB / operationTimeSeconds;
        StreamletsMetrics.PUT_REQUEST_STORAGE_THROUGHPUT_GAUGE.record(transferSpeedMBps);
    }

    private static void recordMultipartUploadMetrics(long startTime, long transferredBytes) {
        double operationTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
        StreamletsMetrics.MULTIPART_UPLOAD_REQUEST_PART_OPERATIONS_COUNTER.incrementCounter();
        double transferSizeMB = (transferredBytes / (1024.0 * 1024.0));
        StreamletsMetrics.MULTIPART_UPLOAD_REQUEST_PART_SIZE_GAUGE.record(transferSizeMB);
        double transferSpeedMBps = transferSizeMB / operationTimeSeconds;
        StreamletsMetrics.MULTIPART_UPLOAD_REQUEST_PART_THROUGHPUT_GAUGE.record(transferSpeedMBps);
    }

    // end region
}