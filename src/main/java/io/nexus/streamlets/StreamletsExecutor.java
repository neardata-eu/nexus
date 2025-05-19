package io.nexus.streamlets;

import io.nexus.streamlets.compiler.StreamletLoader;
import io.nexus.streamlets.context.RequestContext;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.StreamletDescriptor;
import io.nexus.streamlets.metadata.StreamletDescriptor.ExecuteOn;
import io.nexus.streamlets.metadata.StreamletExecutionDescriptor;
import io.nexus.streamlets.state.StreamletStateManager;
import io.nexus.streamlets.utils.FastPipedInputStream;
import io.nexus.streamlets.utils.FastPipedOutputStream;
import io.nexus.streamlets.utils.StreamletIO;
import io.nexus.streamlets.utils.StreamletsCache;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;

import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * This class checks the {@link MetadataService} of Nexus for policies and runs streamlets on requests payloads
 * according to the defined policies.
 */
public class StreamletsExecutor implements Closeable {
    final Logger logger = LoggerFactory.getLogger(StreamletsExecutor.class);
    private final ScheduledExecutorService streamletExecutor;
    private final MetadataService metadataService;
    private final StreamletStateManager stateManager;
    private final MultiKeySequentialProcessor<String> processor;
    private final StreamletsCache streamletsCache;

    public StreamletsExecutor(MetadataService metadataService, StreamletStateManager stateManager) {
        // Create a separate thread pool for executing streamlets
        this.streamletExecutor = ExecutorServiceHelpers.newScheduledThreadPool(40, "streamlet-threadpool");
        this.metadataService = metadataService;
        StreamletLoader streamletLoader = new StreamletLoader(metadataService);
        this.streamletsCache = new StreamletsCache(10000, 1, TimeUnit.HOURS, streamletLoader);
        this.stateManager = stateManager;
        this.processor = new MultiKeySequentialProcessor<>(this.streamletExecutor);
    }

    // region public methods

    /**
     * Adds a storage request to the queue for streamlet processing. The semantics of the queue are that requests to
     * different stream partitions will be processed in parallel. However, requests to the same stream partition will be
     * processed sequentially. This is a requirement to linearize updates to metadata in stateful streamlets.
     *
     * @param policy           The policy defining which streamlets should be executed.
     * @param streamPartition  The stream partition at hand.
     * @param streamletInput   The input stream containing the data to be processed.
     * @param forwardStream    Whether this is a Put or a Get request.
     * @param streamletResult  The output stream where the processed data will be written.
     * @return A {@link CompletableFuture} that completes when the request is fully processed.
     * @throws NoPolicySetException If no streamlets are applicable based on the policy.
     * @throws RuntimeException If an error occurs while retrieving metadata or processing the request.
     */
    public CompletableFuture<Long> processRequest(Policy policy, StreamPartition streamPartition, InputStream streamletInput,
                                                  boolean forwardStream, RequestContext context, OutputStream streamletResult) {
        // TODO: Make sure that getScopedPartitionUri() is what we want, we are working with it in the queue and the cache
        return this.processor.add(Collections.singleton(streamPartition.getScopedPartitionUri()),
                () -> processRequestInternal(policy, streamPartition, streamletInput, forwardStream, context, streamletResult));
    }

    /**
     * Processes a request by executing the streamlets associated with the given policy. If no streamlets are applicable,
     * an exception is thrown. The function runs asynchronously and returns a {@link CompletableFuture} that completes
     * when the request processing is finished.
     *
     * @param policy           The policy defining which streamlets should be executed.
     * @param streamPartition  The stream partition at hand.
     * @param streamletInput   The input stream containing the data to be processed.
     * @param forwardStream    Whether this is a Put or a Get request.
     * @param streamletResult  The output stream where the processed data will be written.
     * @return A {@link CompletableFuture} that completes when the request is fully processed.
     * @throws NoPolicySetException If no streamlets are applicable based on the policy.
     * @throws RuntimeException If an error occurs while retrieving metadata or processing the request.
     */
     CompletableFuture<Long> processRequestInternal(Policy policy, StreamPartition streamPartition, InputStream streamletInput,
                                                  boolean forwardStream, RequestContext context, OutputStream streamletResult) {
        // Getting all streamlets that should be executed based on the applied policy and their metadata.
        List<Streamlet> streamletsToBeExecuted = buildStreamletPipelineFromPolicy(streamPartition, policy, forwardStream);
        if (streamletsToBeExecuted.isEmpty()) {
            // If there are no streamlets to be executed, forward the blob to the next Swarmlet or final destination.
            logger.warn("No streamlets to be executed {}", streamPartition.getScopedObjectName());
            throw new NoPolicySetException("No Streamlets to execute.");

        }
        try {
            CompletableFuture<Long> pipelineFuture = processRequestContent(streamPartition, streamletInput,
                    streamletsToBeExecuted, forwardStream, context, streamletResult);
            logger.info("Submitted streamlets for processing {} based on {}", streamPartition.getScopedObjectName(), policy);
            return pipelineFuture;
        } catch (Exception e) {
            logger.error("Error getting the current blob's metadata.", e);
            throw new RuntimeException(e);
        }
    }

    // end region

    // region private methods

    /**
     * Builds and returns a list of {@link Streamlet} instances that should be executed based on the provided policy.
     * The selection of streamlets depends on the execution direction (PUT or GET) and whether they should be executed
     * for the given request type.
     *
     * @param policy        The policy defining which streamlets should be executed.
     * @param forwardStream {@code true} if the streamlets should be executed in forward order (e.g., for PUT operations),
     *                      {@code false} if the execution should be reversed (e.g., for GET operations).
     * @return A list of {@link Streamlet} instances that should be executed according to the policy.
     * @throws RuntimeException If an error occurs while retrieving streamlets from the policy.
     */
    private List<Streamlet> buildStreamletPipelineFromPolicy(StreamPartition streamPartition, Policy policy, boolean forwardStream) {
        try {
            // Build the execution pipeline of streamlets based on the policy defined in this Region. For PUTs, the
            // pipeline is executed left-to-right. For GETs, the pipeline execution is reversed.
            Region currentRegion = this.metadataService.getNexusConfig().getRegion();
            List<StreamletExecutionDescriptor> policyPipeline = forwardStream ?
                    policy.getStreamletsForRegion(currentRegion) :
                    policy.getStreamletsForRegion(currentRegion).reversed();
            // Check if the streamlet is to be executed as per the request type and return the list of executable functions.
            return policyPipeline.stream()
                    .filter(s -> shouldExecuteStreamletOnRequest(s.getStreamlet(), forwardStream))
                    .map(s -> {
                        // Load state of Streamlet, if any.
                        boolean isCachedStreamlet = this.streamletsCache.exists(streamPartition.getScopedPartitionUri(), s.getStreamlet().getId());
                        Streamlet streamlet = this.streamletsCache.getOrLoadStreamlet(streamPartition.getScopedPartitionUri(), s.getStreamlet().getId());
                        this.stateManager.loadPersistentFields(streamlet, isCachedStreamlet, streamPartition);
                        return streamlet;
                    })
                    .toList();
        } catch (Exception e) {
            logger.debug("Error finding streamlet from policy pipeline");
            throw new RuntimeException(e);
        }
    }

    /**
     * Determines whether a given streamlet should be executed based on the request type and execution policy.
     *
     * @param streamletDescriptor The descriptor of the Streamlet containing execution metadata.
     * @param forwardStream       {@code true} if the request is a forward operation (e.g., PUT),
     *                            {@code false} if it is a retrieval operation (e.g., GET).
     * @return {@code true} if the streamlet should be executed for the given request type, {@code false} otherwise.
     */
    private boolean shouldExecuteStreamletOnRequest(StreamletDescriptor streamletDescriptor, boolean forwardStream) {
        // On GETs, we do not need to execute DataSource and DataRouting streamlets, as they are executed on preGET.
        if (!forwardStream && streamletDescriptor.isDataRouting()) return false;
        // For the rest, check if the type of request is supported.
        ExecuteOn executeOn = streamletDescriptor.getExecuteOn();
        if (executeOn == ExecuteOn.ALL)
            return true;
        if (forwardStream & executeOn == ExecuteOn.PUT)
            return true;
        return !forwardStream && executeOn == ExecuteOn.GET;
    }

    /**
     * Processes the request content by initializing the necessary resources, building the streamlet execution pipeline,
     * and managing concurrent processing of the request data.
     *
     * The function sets up a durable log output stream and a processing pipeline, allowing parallel execution of
     * streamlets while storing and processing content concurrently.
     *
     * @param streamPartition       The partition of the stream where the request is being processed.
     * @param streamletInput        The input stream containing the data to be processed.
     * @param streamletsToBeExecuted A list of Streamlets that should be executed based on the policy.
     * @param forwardStream         {@code true} if the request is a forward operation (e.g., PUT),
     *                              {@code false} if it is a retrieval operation (e.g., GET).
     * @param streamletResult       The output stream where the processed data should be written.
     * @return A {@link CompletableFuture} that completes when both the streamlet processing pipeline and
     *         the storage operation have been completed.
     * @throws RuntimeException If an I/O error occurs during the processing.
     */
    private CompletableFuture<Long> processRequestContent(StreamPartition streamPartition, InputStream streamletInput,
                                                          List<Streamlet> streamletsToBeExecuted, boolean forwardStream, RequestContext context, OutputStream streamletResult) {
        try {
            // Instantiate the input stream for the request contents.
            logger.info("Submitting processing pipeline task for stream partition {}.", streamPartition);
            FastPipedOutputStream transferDataOutput = new FastPipedOutputStream();
            InputStream streamletPipelineInputStream = new FastPipedInputStream(transferDataOutput);
            // Build the pipeline of Streamlets and connect their streams with the one from the interceptor.
            long startTime = System.nanoTime();
            CompletableFuture<Void> pipelineFuture = buildStreamletExecutionPipeline(streamletPipelineInputStream,
                    streamletResult, streamletsToBeExecuted, forwardStream, context);
            StreamletsMetrics.PIPELINE_BUILD_TIMER.record(System.nanoTime() - startTime);
            // Write the request input stream through the Streamlet pipeline.
            CompletableFuture<Long> transferInputDataFuture = CompletableFuture.supplyAsync(() ->
                    transferInputData(streamletInput, transferDataOutput), this.streamletExecutor);
            return pipelineFuture.thenCompose(v -> transferInputDataFuture);
        } catch (IOException e) {
            logger.error("Error processing request's content");
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the streamletInput input stream for feeding the Streamlet pipeline and write to the durableLog in parallel.
     *
     * @param streamletInput Input data from the request.
     * @param streamletPipelineInput Output stream containing the input of the Streamlet pipeline after being stored in
     *                               the durableLog.
     */
    private long transferInputData(InputStream streamletInput, OutputStream streamletPipelineInput) {
        int totalBytesRead = 0;
        int bytesRead;
        final int arraySize = 8192;
        byte[] objectBytes = new byte[arraySize];
        try {
            while ((bytesRead = streamletInput.read(objectBytes)) != -1) {
                logger.debug("Reading {} bytes from the request to feed Streamlet pipeline.", bytesRead);
                // Append the new read data to the input stream of the processing pipeline for concurrent processing.
                streamletPipelineInput.write((bytesRead == arraySize) ? objectBytes :
                        Arrays.copyOfRange(objectBytes, 0, bytesRead));
                totalBytesRead += bytesRead;
            }
            streamletPipelineInput.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        logger.info("Stored and processed {} bytes from the request input stream.", totalBytesRead);
        return totalBytesRead;
    }

    /**
     * Build a list of Streamlets and input and output streams that represent a pipeline to execute. The Streamlets are
     * pipelined in natural order via a doPut() or in reverse order via a goGet() depending on the request type. The
     * input and output streams passed as arguments represent the initial and terminal data streams for the pipeline.
     * The returned future completes when the whole pipeline completes its execution (or fails).
     *
     * @param requestInputStream Input data for the Streamlet pipeline.
     * @param streamletResult Output data for the Streamlet pipeline.
     * @param streamletsToBeExecuted List of Streamlets to be pipelined.
     * @param forwardStream Whether this is a Put or Get request.
     * @return A {@link CompletableFuture} that, when complete, implies that the Streamlet pipeline has completed
     * processing or failed.
     * @throws IOException If a problem occurs when managing the input/output streams.
     */
    private CompletableFuture<Void> buildStreamletExecutionPipeline(InputStream requestInputStream, OutputStream streamletResult,
                                                                    List<Streamlet> streamletsToBeExecuted, boolean forwardStream,
                                                                    RequestContext context) throws IOException {
        // Build the data pipeline that will transfer data across Streamlets.
        List<Map.Entry<InputStream, OutputStream>> dataPipeline = new ArrayList<>();
        for (int i = 0; i < streamletsToBeExecuted.size(); i++) {
            // The first input stream is the one passed by parameter by the durable log writer.
            InputStream input = (i == 0) ? requestInputStream :
                    new FastPipedInputStream((FastPipedOutputStream) dataPipeline.get(i - 1).getValue());
            // The last output stream is the one passed by parameter, which will be either forwarded or stored.
            OutputStream output = (i == streamletsToBeExecuted.size() - 1) ? streamletResult : new FastPipedOutputStream();
            dataPipeline.add(new AbstractMap.SimpleEntry<>(input, output));
        }

        // Submit the Streamlets for execution with their corresponding input/output streams.
        List<CompletableFuture<Void>> pipeline = new ArrayList<>();
        int index = 0;
        for (Streamlet streamlet : streamletsToBeExecuted) {
            StreamletIO dataStreams = new StreamletIO(dataPipeline.get(index).getKey(), dataPipeline.get(index).getValue());
            index++;
            // Based on the pipeline flow, execute each Streamlet's PUT/GET accordingly.
            BiConsumer<StreamletIO, RequestContext> currentStreamlet = forwardStream ? streamlet::handlePut : streamlet::handleGet;
            pipeline.add(CompletableFuture.runAsync(() -> currentStreamlet.accept(dataStreams, context), this.streamletExecutor));
        }
        // Store streamlet state after pipeline execution.
        return Futures.allOf(pipeline).thenAccept(v -> streamletsToBeExecuted.forEach(s ->
                this.stateManager.savePersistentFields(s, context.getStreamPartition())));
    }

    public CompletableFuture<Void> getPreGetTransferFuture(Policy policy, Region currentRegion, StreamPartition streamPartition,
                                                           StreamletContext context, FastPipedOutputStream streamletInputOutputStream) {
        if (!policy.hasDataRoutingStreamlet(currentRegion)) {
            // No data source streamlet, skipping.
            return null;
        }
        // Instantiate the data source streamlet for this region and partition.
        boolean isCachedStreamlet = this.streamletsCache.exists(streamPartition.getScopedPartitionUri(),
                policy.getDataRoutingStreamletId(currentRegion).get());
        DataSourceStreamlet dataSourceStreamlet = (DataSourceStreamlet) this.streamletsCache
                .getOrLoadStreamlet(streamPartition.getScopedPartitionUri(), policy.getDataRoutingStreamletId(currentRegion).get());
        this.stateManager.loadPersistentFields(dataSourceStreamlet, isCachedStreamlet, streamPartition);
        logger.info("Executing pre-GET for Streamlet {}", dataSourceStreamlet);
        InputStream preGetInputStream = dataSourceStreamlet.handlePreGet(streamPartition, context);
        return preGetInputStream == null ? null : CompletableFuture.runAsync(() ->
                    transferInputData(preGetInputStream, streamletInputOutputStream), this.streamletExecutor);
    }

    @Override
    public void close() throws IOException {
        this.processor.close();
        ExecutorServiceHelpers.shutdown(this.streamletExecutor);
    }

    // end region
}
