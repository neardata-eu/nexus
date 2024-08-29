package io.nexus.streamlets;

import io.nexus.streamlets.durablelog.DurableLog;
import io.nexus.streamlets.durablelog.FileSystemDurableLog;
import io.nexus.streamlets.utils.DynamicInputStream;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.util.ByteArraySegment;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.*;
import org.jclouds.blobstore.options.*;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.domain.Location;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class StreamletsMiddleware extends ForwardingBlobStore {
    private static final Logger logger = LoggerFactory.getLogger(StreamletsMiddleware.class);
    private final ScheduledExecutorService streamletExecutor;
    private final MultiKeySequentialProcessor<String> taskScheduler;
    private final DurableLog durableLog;

    public StreamletsMiddleware(BlobStore blobStore) {
        super(blobStore);

        // Create a separate threadpool for executing streamlets.
        this.streamletExecutor = ExecutorServiceHelpers.newScheduledThreadPool(10, "streamlet-threadpool");
        this.taskScheduler = new MultiKeySequentialProcessor<>(streamletExecutor);
        this.durableLog = new FileSystemDurableLog();
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

            // Create the resources for storing the input PUT request contents.
            logger.info("Creating durable log object.");
            durableLog.createLogObject(streamPartition);

            // Instantiate the dynamic input stream that allows tasks to read contents while still writing.
            logger.info("Submitting processing pipeline task for stream partition {}.", streamPartition);
            DynamicInputStream dynamicInputStream = new DynamicInputStream();
            streamletPipelineResult = this.taskScheduler.add(Collections.singletonList(streamPartition),
                    () -> CompletableFuture.supplyAsync(() -> functionOne(dynamicInputStream), this.streamletExecutor));

            // In parallel, the main thread (IO thread pool) can write the storage operation to the log, whereas
            // we schedule the execution of the function pipeline in the streamlet pool.
            InputStream payload = blob.getPayload().openStream();
            byte[] objectBytes = new byte[10];
            int totalBytesRead = 0;
            while (totalBytesRead < payload.available()) {
                int bytesRead = payload.read(objectBytes, 0, objectBytes.length);
                logger.debug("Reading {} bytes from the request input stream.", bytesRead);
                if (bytesRead < 0) {
                    // End of stream/
                    break;
                }
                // Append the new read data to the input stream of the processing pipeline for concurrent processing.
                dynamicInputStream.addSegment(new ByteArraySegment(objectBytes, 0, bytesRead));
                // Write the new read data to log service for failure recovery.
                this.durableLog.writeToLogObject(streamPartition, objectBytes, bytesRead);
                totalBytesRead += bytesRead;
            }
            this.durableLog.closeLogObject(streamPartition);
            // Important: we need to close the dynamicInputStream, so the streamlets know we completed reading.
            dynamicInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // If needed, wait for the streamlet pipeline processing result to use it to continue data routing.
        InputStream result = streamletPipelineResult.join();
        blob.setPayload(result);

        // Reply to the client once the storage log storage completes, and forward the operation to the next stage.
        return super.putBlob(containerName, blob, putOptions);
    }

    private InputStream functionOne(DynamicInputStream input) {

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
