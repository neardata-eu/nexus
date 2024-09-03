package io.nexus.streamlets;

import io.nexus.streamlets.durablelog.DurableLog;
import io.nexus.streamlets.durablelog.FileSystemDurableLog;
import io.nexus.streamlets.functions.NoOpStreamlet;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.util.ByteArraySegment;
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
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class StreamletsMiddleware extends ForwardingBlobStore {
    private static final Logger logger = LoggerFactory.getLogger(StreamletsMiddleware.class);
    private final ScheduledExecutorService streamletExecutor;
    private final MultiKeySequentialProcessor<String> taskScheduler;
    private final DurableLog durableLog;
    private final Map<String, BiConsumer<ByteBufferPipelineStream, ByteBufferPipelineStream>> functionSupplierMap;

    public StreamletsMiddleware(BlobStore blobStore) {
        super(blobStore);

        // Create a separate threadpool for executing streamlets.
        this.streamletExecutor = ExecutorServiceHelpers.newScheduledThreadPool(10, "streamlet-threadpool");
        this.taskScheduler = new MultiKeySequentialProcessor<>(streamletExecutor);
        this.durableLog = new FileSystemDurableLog();
        this.functionSupplierMap = new HashMap<>();
        this.functionSupplierMap.put("noop-1", new NoOpStreamlet("noop-1")::processPut);
        this.functionSupplierMap.put("noop-2", new NoOpStreamlet("noop-2")::processPut);
        this.functionSupplierMap.put("noop-3", new NoOpStreamlet("noop-3")::processPut);
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
    public boolean createContainerInLocation(Location location, String container, CreateContainerOptions createContainerOptions) {
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
    public String putBlob(String containerName, Blob blob) {
        return super.putBlob(containerName, blob);
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
        // 1. TODO: Validate credentials of incoming request to make sure it is a valid one.
        // 2. TODO: Check if there is any policy to apply to this storage operation.
        // 3. TODO: If there is no policy, just forward the storage to the next swarmlet or final destination.

        final CompletableFuture<InputStream> streamletPipelineResult;
        try {
            // TODO: Get the partition name correctly.
            String streamPartition = "/tmp/test.txt";
            // TODO: Get this from metadata.
            List<String> streamletPipelineFromPolicy = streamletPipelineFromPolicyMetadata();

            // Create the resources for storing the input PUT request contents.
            initializeDurableLogObject(streamPartition);

            // Instantiate the input stream for the request contents.
            logger.info("Submitting processing pipeline task for stream partition {}.", streamPartition);
            ByteBufferPipelineStream requestInputStream = new ByteBufferPipelineStream();

            // Build the execution pipeline of streamlets based on the policy defined.
            streamletPipelineResult = buildStreamletExecutionPipeline(requestInputStream, streamletPipelineFromPolicy,
                    streamPartition);

            // In parallel, the main thread (IO thread pool) can write the storage operation to the log, whereas
            // we schedule the execution of the function pipeline in the streamlet pool.
            InputStream payload = blob.getPayload().openStream();

            int totalBytesRead = 0;
            long contentLength = blob.getMetadata().getContentMetadata().getContentLength();
            int iniByte = 0;
            int endByte = 0;
            int iteration = 0;
            while (totalBytesRead < contentLength) {
                byte[] objectBytes = new byte[8192];
                int bytesRead = payload.read(objectBytes);
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
                iniByte = endByte;
                endByte += bytesRead;
                iteration++;
            }
            this.durableLog.closeLogObject(streamPartition);
            // Important: we need to close the dynamicInputStream, so the streamlets know we completed reading.
            requestInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // If needed, wait for the streamlet pipeline processing result to use it to continue data routing.
        InputStream result = streamletPipelineResult.join();
        blob.setPayload(result);

        // Reply to the client once the storage log storage completes, and forward the operation to the next stage.
        return super.putBlob(containerName, blob, putOptions);
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

    // TODO: Do this for real
    private List<String> streamletPipelineFromPolicyMetadata() {
        List<String> streamletPipelineFromPolicy = new ArrayList<>();
        streamletPipelineFromPolicy.add("noop-1");
        streamletPipelineFromPolicy.add("noop-2");
        streamletPipelineFromPolicy.add("noop-3");
        return streamletPipelineFromPolicy;
    }

    private void initializeDurableLogObject(String streamPartition) {
        logger.info("Creating durable log object.");
        this.durableLog.createLogObject(streamPartition);
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
    public Blob getBlob(String containerName, String blobName) {
        return super.getBlob(containerName, blobName);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        return super.getBlob(containerName, blobName, getOptions);
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
        return super.initiateMultipartUpload(container, blobMetadata, options);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        super.abortMultipartUpload(mpu);
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
        return super.completeMultipartUpload(mpu, parts);
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
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
}
