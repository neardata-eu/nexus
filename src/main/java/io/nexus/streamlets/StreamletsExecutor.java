package io.nexus.streamlets;

import io.nexus.shared.metrics.TimerMetric;
import io.nexus.streamlets.context.RequestContext;
import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.deserializers.StringDeserializer;
import io.nexus.streamlets.durablelog.DurableLog;
import io.nexus.streamlets.durablelog.FileSystemDurableLog;
import io.nexus.streamlets.functions.CompressionStreamlet;
import io.nexus.streamlets.functions.NoOpStreamlet;
import io.nexus.streamlets.functions.WordCountStreamlet;
import io.nexus.streamlets.metadata.*;
import io.nexus.streamlets.metadata.StreamletDescriptor.ExecuteOn;
import io.nexus.streamlets.utils.FastPipedInputStream;
import io.nexus.streamlets.utils.FastPipedOutputStream;
import io.nexus.streamlets.utils.StreamletIO;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
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
    private final DurableLog durableLog;
    private final MetadataService metadataService;
    private final Map<String, Streamlet> functionSupplierMap;

    public StreamletsExecutor(MetadataService metadataService) {
        // Create a separate thread pool for executing streamlets
        this.streamletExecutor = ExecutorServiceHelpers.newScheduledThreadPool(10, "streamlet-threadpool");
        this.durableLog = new FileSystemDurableLog();
        this.metadataService = metadataService;
        this.functionSupplierMap = new HashMap<>();
        // TODO: Dynamically load Streamlets from Redis source code and instantiate one per partition
        this.functionSupplierMap.put("noop-1", new NoOpStreamlet("NOOP"));
        this.functionSupplierMap.put("compression-1", new CompressionStreamlet("COMPRESSION-1"));
        this.functionSupplierMap.put("wordcount-1", new WordCountStreamlet(new StringDeserializer()));
        this.functionSupplierMap.put("compression-2", new CompressionStreamlet("COMPRESSION-2"));
    }

    // region public methods

    @VisibleForTesting
    public Map<String, Streamlet> getFunctionSupplierMap() {
        return functionSupplierMap;
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
    public CompletableFuture<Long> processRequest(Policy policy, StreamPartitionPojo streamPartition, InputStream streamletInput,
                                                  boolean forwardStream, RequestContext context, OutputStream streamletResult) {
        // Getting all streamlets that should be executed based on the applied policy and their metadata.
        List<Streamlet> streamletsToBeExecuted = fillStreamletPipelineFromPolicy(policy, forwardStream);
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
    private List<Streamlet> fillStreamletPipelineFromPolicy(Policy policy, boolean forwardStream) {
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
                    .map(s -> this.functionSupplierMap.get(s.getStreamlet().getId()))
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
        ExecuteOn executeOn = streamletDescriptor.getExecuteOn();
        if (executeOn == ExecuteOn.ALL)
            return true;
        if (forwardStream & executeOn == ExecuteOn.PUT)
            return true;
        if (!forwardStream & executeOn == ExecuteOn.GET)
            return true;

        return false;
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
    private CompletableFuture<Long> processRequestContent(StreamPartitionPojo streamPartition, InputStream streamletInput,
            List<Streamlet> streamletsToBeExecuted, boolean forwardStream, RequestContext context, OutputStream streamletResult) {
        try {
            // Create the resources for storing the input PUT request contents.
            String scopedPartitionName = streamPartition.getScopedObjectName();
            initializeDurableLogObject(streamPartition);
            // Instantiate the input stream for the request contents.
            logger.info("Submitting processing pipeline task for stream partition {}.", streamPartition);
            FastPipedOutputStream durableLogOutput = new FastPipedOutputStream();
            InputStream streamletPipelineInputStream = new FastPipedInputStream(durableLogOutput);
            // Build the pipeline of Streamlets and connect their streams with the one from the interceptor.
            long startTime = System.nanoTime();
            CompletableFuture<Void> pipelineFuture = buildStreamletExecutionPipeline(streamletPipelineInputStream,
                    streamletResult, streamletsToBeExecuted, forwardStream, context);
            StreamletsMetrics.PIPELINE_BUILD_TIMER.record(System.nanoTime() - startTime);
            // In parallel, the main thread (IO thread pool) can write the storage operation to the log,
            // whereas we schedule the execution of the function pipeline in the streamlet pool.
            CompletableFuture<Long> durableLogFuture = CompletableFuture.supplyAsync(() ->
                    storeAndProcessContent(streamletInput, durableLogOutput, scopedPartitionName), this.streamletExecutor);
            return pipelineFuture.thenCompose(v -> durableLogFuture);
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
     * @param streamPartition Name for the stream partition at hand.
     */
    private long storeAndProcessContent(InputStream streamletInput, OutputStream streamletPipelineInput, String streamPartition) {
        int totalBytesRead = 0;
        int bytesRead;
        final int arraySize = 8192;
        byte[] objectBytes = new byte[arraySize];
        try {
            while ((bytesRead = streamletInput.read(objectBytes)) != -1) {
                logger.debug("Reading {} bytes from the request to feed Streamlet pipeline and durableLog.", bytesRead);
                // Append the new read data to the input stream of the processing pipeline for concurrent processing.
                streamletPipelineInput.write((bytesRead == arraySize) ? objectBytes :
                        Arrays.copyOfRange(objectBytes, 0, bytesRead));
                // Write the new read data to log service for failure recovery.
                this.durableLog.writeToLogObject(streamPartition, objectBytes, bytesRead);
                totalBytesRead += bytesRead;
            }
            this.durableLog.closeLogObject(streamPartition);
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
        return Futures.allOf(pipeline);
    }

    private void initializeDurableLogObject(StreamPartitionPojo streamPartition) {
        logger.info("Creating durable log object.");
        this.durableLog.createLogObject(streamPartition);
    }

    // end region
}
