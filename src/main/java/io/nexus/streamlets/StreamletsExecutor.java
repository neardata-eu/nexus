package io.nexus.streamlets;

import io.nexus.streamlets.durablelog.DurableLog;
import io.nexus.streamlets.durablelog.FileSystemDurableLog;
import io.nexus.streamlets.functions.NoOpStreamlet;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.nexus.streamlets.utils.StreamNameUtils;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.util.ByteArraySegment;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
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
 * This class checks the {@link MetadataService} of Nexus for policies and runs streamlets on requests
 * payloads according to the defined policies. Moreover, in parallel to the execution of streamlets,
 * this class also writes to the {@link DurableLog} the incoming request. This is needed to perform
 * retries in case of failures within a data management processing pipeline.
 */
public class StreamletsExecutor {

    private static final Logger logger = LoggerFactory.getLogger(StreamletsExecutor.class);
    private final ScheduledExecutorService streamletExecutor;
    private final MultiKeySequentialProcessor<String> taskScheduler;
    private final DurableLog durableLog;
    private final MetadataService metadataService;
    private final Map<String, BiConsumer<ByteBufferPipelineStream, ByteBufferPipelineStream>> functionSupplierMap;

    public StreamletsExecutor(MetadataService metadataService) {
        // Create a separate thread pool for executing streamlets.
        this.streamletExecutor = ExecutorServiceHelpers.newScheduledThreadPool(10, "streamlet-threadpool");
        this.taskScheduler = new MultiKeySequentialProcessor<>(streamletExecutor);
        this.durableLog = new FileSystemDurableLog();
        this.metadataService = metadataService;
        this.functionSupplierMap = new HashMap<>();
        // TODO: We need to properly load functions
        this.functionSupplierMap.put("noop-1", new NoOpStreamlet("noop-1")::doTransform);
        this.functionSupplierMap.put("noop-2", new NoOpStreamlet("noop-2")::doTransform);
        this.functionSupplierMap.put("noop-3", new NoOpStreamlet("noop-3")::doTransform);
    }

    // region public methods

    public void interceptAndProcessPut(String containerName, Blob blob, PutOptions putOptions) {
        // 1. TODO: Validate credentials of incoming request to make sure it is a valid one.
        // Check if there is any policy to apply to this storage operation.
        String scope = StreamNameUtils.getScopeFromRequest(blob);
        String stream = StreamNameUtils.getStreamFromRequest(blob);
        if (scope == null || stream == null) {
            // Problem parsing the object path, skipping.
            logger.warn("Malformed object name being intercepted {}", blob.getMetadata().getName());
            return;
        }
        Policy policy = getPolicyForStream(scope, stream);
        if (policy == null) {
            // If there is no policy, just forward the storage to the next swarmlet or final destination.
            logger.debug("No policy set for scope/stream of object {}", blob.getMetadata().getName());
            return;
        }
        final long contentLength = blob.getMetadata().getContentMetadata().getContentLength();
        if (contentLength > 0) {
            final Payload payload = blob.getPayload();
            // TODO: Get the partition name correctly.
            StreamPartitionPojo streamPartition = StreamPartitionPojo.getStreamPartitionPojo(
                    blob.getMetadata().getName(), policy.getSystem(), containerName);
            InputStream processedContent = interceptAndProcessUpload(streamPartition, contentLength, payload, policy);
            blob.setPayload(processedContent);
        }
    }

    public Payload interceptAndProcessMultipartUpload(MultipartUpload multipartUpload, int partNumber, Payload payload) {
        // 1. TODO: Validate credentials of incoming request to make sure it is a valid one.
        // Check if there is any policy to apply to this storage operation.
        String scope = StreamNameUtils.getScopeFromRequest(multipartUpload);
        String stream = StreamNameUtils.getStreamFromRequest(multipartUpload);
        if (scope == null || stream == null) {
            // Problem parsing the object path, skipping.
            logger.warn("Malformed object name being intercepted {}", multipartUpload.blobName());
            return payload;
        }
        Policy policy = getPolicyForStream(scope, stream);
        if (policy == null) {
            // If there is no policy, just forward the storage to the next swarmlet or final destination.
            logger.debug("No policy set for scope/stream of object {}", multipartUpload.blobName());
            return payload;
        }
        // 3. TODO: If there is no policy, just forward the storage to the next swarmlet or final destination.
        final long contentLength = payload.getContentMetadata().getContentLength();
        StreamPartitionPojo streamPartition = StreamPartitionPojo.buildStreamPartitionPojoFromKafkaRequestPath(
                multipartUpload.blobName(), multipartUpload.containerName());
        InputStream processedContent = interceptAndProcessUpload(streamPartition, contentLength, payload, policy);
        return Payloads.newInputStreamPayload(processedContent);
    }

    // end region

    // region private methods

    private Policy getPolicyForStream(String scopeName, String streamName) {
        try {
            // First, check if there is any policy for this specific stream.
            Policy policy = this.metadataService.getPolicyByStream(scopeName, streamName);
            // If there is a policy for this stream, return it. If not, look for any scope-level policy.
            return (policy != null) ? policy : this.metadataService.getPolicyByScope(scopeName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream interceptAndProcessUpload(StreamPartitionPojo streamPartition, long contentLength, Payload payload,
                                                  Policy policy) {

        final CompletableFuture<InputStream> streamletPipelineResult;
        try {
            // Create the resources for storing the input PUT request contents.
            String scopedPartitionName = streamPartition.getScopedObjectName();
            initializeDurableLogObject(streamPartition);

            // Instantiate the input stream for the request contents.
            logger.info("Submitting processing pipeline task for stream partition {}.", streamPartition);
            ByteBufferPipelineStream requestInputStream = new ByteBufferPipelineStream();

            // Build the execution pipeline of streamlets based on the policy defined.
            streamletPipelineResult = buildStreamletExecutionPipeline(requestInputStream, policy.getPipeline(),
                    scopedPartitionName);

            // In parallel, the main thread (IO thread pool) can write the storage operation to the log, whereas
            // we schedule the execution of the function pipeline in the streamlet pool.
            storeAndProcessContent(contentLength, payload, requestInputStream, scopedPartitionName);

            this.durableLog.closeLogObject(scopedPartitionName);
            // Important: we need to close the dynamicInputStream, so the streamlets know we completed reading.
            requestInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // If needed, wait for the streamlet pipeline processing result to use it to continue data routing.
        return streamletPipelineResult.join();
    }

    private void storeAndProcessContent(long contentLength, Payload payload,
                                        ByteBufferPipelineStream requestInputStream,
                                        String streamPartition) throws IOException {
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
            // Append the new read data to the input stream of the processing pipeline for concurrent processing.
            requestInputStream.addSegment(readData);
            // Write the new read data to log service for failure recovery.
            this.durableLog.writeToLogObject(streamPartition, readData.array(), bytesRead);
            totalBytesRead += bytesRead;
        }
    }

    private CompletableFuture<InputStream> buildStreamletExecutionPipeline(ByteBufferPipelineStream requestInputStream,
                                                                           List<String> streamletPipelineFromPolicy,
                                                                           String streamPartition) {
        // Start with the initial input wrapped in a CompletableFuture
        List<Map.Entry<ByteBufferPipelineStream, ByteBufferPipelineStream>> pipelineStreams = new ArrayList<>();
        Map.Entry<ByteBufferPipelineStream, ByteBufferPipelineStream> streamTuple =
                new AbstractMap.SimpleEntry<>(requestInputStream, new ByteBufferPipelineStream());
        for (String s : streamletPipelineFromPolicy) {
            pipelineStreams.add(streamTuple);
            streamTuple = new AbstractMap.SimpleEntry<>(streamTuple.getValue(), new ByteBufferPipelineStream());
        }

        List<CompletableFuture<Void>> pipeline = new ArrayList<>();
        int index = 0;
        AtomicReference<ByteBufferPipelineStream> resultStream = new AtomicReference<>();
        for (String s : streamletPipelineFromPolicy) {
            ByteBufferPipelineStream input = pipelineStreams.get(index).getKey();
            ByteBufferPipelineStream output = pipelineStreams.get(index).getValue();
            index++;
            BiConsumer<ByteBufferPipelineStream, ByteBufferPipelineStream> streamlet = this.functionSupplierMap.get(s);
            pipeline.add(CompletableFuture.runAsync(() -> streamlet.accept(input, output), this.streamletExecutor));
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
