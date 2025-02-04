package io.nexus.streamlets;

import io.nexus.streamlets.context.ContextManager;
import io.nexus.streamlets.context.RequestContext;
import io.nexus.streamlets.context.StreamletContext;
import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.durablelog.DurableLog;
import io.nexus.streamlets.durablelog.FileSystemDurableLog;
import io.nexus.streamlets.functions.NoOpStreamlet;
import io.nexus.streamlets.metadata.*;
import io.nexus.streamlets.metadata.StreamletDescriptor.ExecuteOn;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.nexus.streamlets.utils.InputStreamRecord;
import io.nexus.streamlets.utils.StreamNameUtils;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.util.ByteArraySegment;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * This class checks the {@link MetadataService} of Nexus for policies and runs
 * streamlets on requests payloads according to the defined policies. Moreover,
 * in parallel to the execution of streamlets, this class also writes to the
 * {@link DurableLog} the incoming request. This is needed to perform retries in
 * case of failures within a data management processing pipeline.
 */
public class StreamletsExecutor {

    final Logger logger = LoggerFactory.getLogger(StreamletsExecutor.class);
    private final ScheduledExecutorService streamletExecutor;
    private final MultiKeySequentialProcessor<String> taskScheduler;
    private final DurableLog durableLog;
    private final MetadataService metadataService;
    // TODO: populate the map with the streamlets, and we have to do it one
    // streamlet instance per stream partition
    private final Map<String, Streamlet> functionSupplierMap;

    public StreamletsExecutor(MetadataService metadataService) {
        // Create a separate thread pool for executing streamlets
        this.streamletExecutor = ExecutorServiceHelpers.newScheduledThreadPool(10, "streamlet-threadpool");
        this.taskScheduler = new MultiKeySequentialProcessor<>(streamletExecutor);
        this.durableLog = new FileSystemDurableLog();
        this.metadataService = metadataService;
        this.functionSupplierMap = new HashMap<>();
        // TODO: Dynamically load Streamlets from Redis source code
        this.functionSupplierMap.put("noop-1", new NoOpStreamlet("NOOP"));
    }

    // region public methods

    @VisibleForTesting
    public Map<String, Streamlet> getFunctionSupplierMap() {
        return functionSupplierMap;
    }

    // Function to intercept both PUT and GET requests depending on "forwardStream"
    public void processRequest(Policy policy, String containerName, Blob blob, boolean forwardStream) {
        // Getting all streamlets that should be executed based on the applied policy
        // and their metadata.
        List<Streamlet> streamletsToBeExecuted = fillStreamletPipelineFromPolicy(policy, forwardStream);
        if (streamletsToBeExecuted.size() == 0) {
            // If there are no streamlets to be executed, forward the blob to the next
            // swarmlet or final destination.
            logger.warn("No streamlets to be executed {}", blob.getMetadata().getName());
            return;
        }

        try {
            final long contentLength = blob.getMetadata().getContentMetadata().getContentLength();
            if (contentLength > 0) {
                final Payload payload = blob.getPayload();
                // TODO: Get the partition name correctly.
                StreamPartitionPojo streamPartition = StreamPartitionPojo
                        .getStreamPartitionPojo(blob.getMetadata().getName(), policy.getSystem(), containerName);
                InputStream processedContent = processRequestContent(streamPartition, contentLength, payload,
                        streamletsToBeExecuted, forwardStream, policy, blob);
                blob.setPayload(processedContent);
                // Content length is needed by S3Proxy when managing data against S3.
                blob.getMetadata().getContentMetadata().setContentLength((long) processedContent.available());
                logger.info("Successfully processed content {} based on {}", blob.getMetadata().getName(), policy);
                return;
            }
            logger.warn("No blob content to process");
        } catch (Exception e) {
            logger.error("Error getting the current blob's metadata.", e);
        }
    }

    public Payload interceptAndProcessMultipartUpload(MultipartUpload multipartUpload, int partNumber,
            Payload payload) {
        // 1. TODO: Validate credentials of incoming request to make sure it is a valid
        // one.
        // Check if there is any policy to apply to this storage operation.
        String scope = StreamNameUtils.getScopeFromRequest(multipartUpload);
        String stream = StreamNameUtils.getStreamFromRequest(multipartUpload);

        if (scope == null || stream == null) {
            // Problem parsing the object path, skipping.
            // Reset the working context
            logger.warn("Malformed object name being intercepted {}", multipartUpload.blobName());
            return payload;
        }

        long startTime = System.nanoTime();
        Policy policy = this.metadataService.getPolicyByStream(scope, stream);
        if (policy == null) {
            // If there is no policy, just forward the storage to the next swarmlet or final
            // destination and reset the context
            logger.warn("No policy set for scope/stream of object {}", multipartUpload.blobName());
            return payload;
        }
        StreamletsMetrics.POLICY_RETRIEVAL_TIMER.record(System.nanoTime() - startTime);

        // Getting all streamlets that should be executed based
        // on the applied policy and their metadata
        List<Streamlet> streamletsToBeExecuted = fillStreamletPipelineFromPolicy(policy, true);
        if (streamletsToBeExecuted.size() == 0) {
            // If there are no streamlets to be executed, forward the payload to the next
            // swarmlet or final destination.
            logger.warn("No streamlets to be executed {}", multipartUpload.blobName());
            return payload;
        }

        // Due to the multipart function not having a blob, this temporary blob is
        // passed as a dummy object to bypass the prop error. This will be resolved when
        // the multipart support is implemented and all requests are normal PUTs
        // TODO: remove this blob when multipart support is implemented
        BlobStoreContext context = ContextBuilder.newBuilder("transient").build(BlobStoreContext.class);
        Blob tempBlob = context.getBlobStore().blobBuilder("tempBlob").build();

        try {
            final long contentLength = payload.getContentMetadata().getContentLength();
            if (contentLength > 0) {
                StreamPartitionPojo streamPartition = StreamPartitionPojo.getStreamPartitionPojo(
                        multipartUpload.blobName(), policy.getSystem(), multipartUpload.containerName());
                InputStream processedContent = processRequestContent(streamPartition, contentLength, payload,
                        streamletsToBeExecuted, true, policy, tempBlob);
                logger.info("Successfully processed content based on {}", policy);
                // Manually setting the processed payload length due to JClouds metadata checks
                // Later down the multipart upload flow
                // TODO: Revisit this implementation when content length varies
                Payload interceptedContent = Payloads.newInputStreamPayload(processedContent);
                interceptedContent.getContentMetadata().setContentLength(contentLength);
                return interceptedContent;
            }
            logger.warn("No multipart upload content to process");
            return payload;

        } catch (Exception e) {
            logger.warn("Error getting the current payload's metadata");
            return payload;
        }
    }

    // end region

    // region private methods

    private List<Streamlet> fillStreamletPipelineFromPolicy(Policy policy, boolean forwardStream) {
        try {
            // Build the execution pipeline of streamlets based on the policy defined in
            // this Region. For PUTs, the
            // pipeline is executed left-to-right. For GETs, the pipeline execution is
            // reversed.
            Region currentRegion = this.metadataService.getNexusConfig().getRegion();
            List<StreamletExecutionDescriptor> policyPipeline = forwardStream
                    ? policy.getStreamletsForRegion(currentRegion)
                    : policy.getStreamletsForRegion(currentRegion).reversed();
            // Check if the streamlet is to be executed as per the request type and return
            // the list of executable functions.
            return policyPipeline.stream().filter(s -> shouldExecuteStreamletOnRequest(s.getStreamlet(), forwardStream))
                    .map(s -> this.functionSupplierMap.get(s.getStreamlet().getId())).toList();
        } catch (Exception e) {
            logger.debug("Error finding streamlet from policy pipeline");
            throw new RuntimeException(e);
        }
    }

    private boolean shouldExecuteStreamletOnRequest(StreamletDescriptor streamletDescriptor, boolean forwardStream) {
        ExecuteOn executeOn = streamletDescriptor.getExecuteOn();
        if (executeOn == ExecuteOn.ALL)
            return true;
        if (forwardStream & executeOn == ExecuteOn.PUT)
            return true;
        if (!forwardStream & executeOn == ExecuteOn.GET)
            return true;

        return false;
    }

    private InputStream processRequestContent(StreamPartitionPojo streamPartition, long contentLength, Payload payload,
            List<Streamlet> streamletsToBeExecuted, boolean forwardStream, Policy policy, Blob blob) {

        final CompletableFuture<InputStream> streamletPipelineResult;

        try {
            // Create the resources for storing the input PUT request contents.
            String scopedPartitionName = streamPartition.getScopedObjectName();
            initializeDurableLogObject(streamPartition);

            // Instantiate the input stream for the request contents.
            logger.info("Submitting processing pipeline task for stream partition {}.", streamPartition);
            ByteBufferPipelineStream requestInputStream = new ByteBufferPipelineStream();

            long startTime = System.nanoTime();

            streamletPipelineResult = buildStreamletExecutionPipeline(requestInputStream, streamletsToBeExecuted,
                    scopedPartitionName, forwardStream, policy, blob);

            StreamletsMetrics.PIPELINE_BUILD_TIMER.record(System.nanoTime() - startTime);

            // In parallel, the main thread (IO thread pool) can write the storage operation
            // to the log,
            // whereas we schedule the execution of the function pipeline in the streamlet
            // pool.
            storeAndProcessContent(contentLength, payload, requestInputStream, scopedPartitionName);

            this.durableLog.closeLogObject(scopedPartitionName);
            // Important: we need to close the dynamicInputStream, so the streamlets know we
            // completed reading.
            requestInputStream.close();

        } catch (IOException e) {
            logger.error("Error processing request's content");
            throw new RuntimeException(e);
        }
        // If needed, wait for the streamlet pipeline processing result to use it to
        // continue data routing.
        return streamletPipelineResult.join();
    }

    private void storeAndProcessContent(long contentLength, Payload payload,
            ByteBufferPipelineStream requestInputStream, String streamPartition) throws IOException {
        InputStream payloadInputStream = payload.openStream();
        int totalBytesRead = 0;

        while (totalBytesRead < contentLength) {
            // TODO: See if there is a more efficient way to read this
            byte[] objectBytes = new byte[8192];
            int bytesRead = payloadInputStream.read(objectBytes);
            logger.debug("Reading {} bytes from the request input stream.", bytesRead);
            if (bytesRead < 0) {
                // End of stream/
                break;
            }
            ByteArraySegment readData = new ByteArraySegment(objectBytes, 0, bytesRead);
            // Append the new read data to the input stream of the processing pipeline for
            // concurrent processing.
            requestInputStream.addSegment(readData);
            // Write the new read data to log service for failure recovery.
            this.durableLog.writeToLogObject(streamPartition, readData.array(), bytesRead);
            totalBytesRead += bytesRead;
        }
    }

    private CompletableFuture<InputStream> buildStreamletExecutionPipeline(ByteBufferPipelineStream requestInputStream,
            List<Streamlet> streamletsToBeExecuted, String streamPartition, boolean forwardStream, Policy policy,
            Blob blob) {

        // Instantiate a context for the pipeline
        final ContextManager contextManager = ContextManager.getInstance();
        RequestContext streamletContext = contextManager.createRequestStreamletContext(logger, policy, blob);

        // Start with the initial input wrapped in a CompletableFuture
        List<Map.Entry<ByteBufferPipelineStream, ByteBufferPipelineStream>> pipelineStreams = new ArrayList<>();
        Map.Entry<ByteBufferPipelineStream, ByteBufferPipelineStream> streamTuple = new AbstractMap.SimpleEntry<>(
                requestInputStream, new ByteBufferPipelineStream());
        for (Streamlet s : streamletsToBeExecuted) {
            pipelineStreams.add(streamTuple);
            streamTuple = new AbstractMap.SimpleEntry<>(streamTuple.getValue(), new ByteBufferPipelineStream());
        }

        List<CompletableFuture<Void>> pipeline = new ArrayList<>();
        int index = 0;
        AtomicReference<ByteBufferPipelineStream> resultStream = new AtomicReference<>();
        // TODO: Support different types of events
        for (Streamlet streamlet : streamletsToBeExecuted) {
            ByteBufferPipelineStream input = pipelineStreams.get(index).getKey();
            ByteBufferPipelineStream output = pipelineStreams.get(index).getValue();
            index++;
            // Based on the pipeline flow, execute each streamlet's PUT/GET correspondingly
            BiConsumer<InputStreamRecord, StreamletContext> currentStreamlet = forwardStream ? streamlet::handlePut
                    : streamlet::handleGet;

            pipeline.add(CompletableFuture.runAsync(
                    () -> currentStreamlet.accept(new InputStreamRecord(input, output), streamletContext),
                    this.streamletExecutor));
            resultStream.set(output);
        }

        CompletableFuture<Void> combinedPipeline = Futures.allOf(pipeline);
        return this.taskScheduler.add(Collections.singletonList(streamPartition),
                () -> combinedPipeline.thenApply(v -> resultStream.get()));
    }

    private void initializeDurableLogObject(StreamPartitionPojo streamPartition) {
        logger.info("Creating durable log object.");
        this.durableLog.createLogObject(streamPartition);
    }

    // end region
}
