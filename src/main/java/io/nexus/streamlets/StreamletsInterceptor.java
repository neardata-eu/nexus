package io.nexus.streamlets;

import io.nexus.streamlets.metadata.MetadataService;

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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * S3 Proxy middleware that intercepts storage requests and injects them into
 * {@link StreamletsExecutor}.
 */
public class StreamletsInterceptor extends ForwardingBlobStore {

    final Logger logger = LoggerFactory.getLogger(StreamletsInterceptor.class);
    private final StreamletsExecutor streamletsExecution;

    public StreamletsInterceptor(BlobStore blobStore, MetadataService metadataService) {
        super(blobStore);
        this.streamletsExecution = new StreamletsExecutor(metadataService);
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
    public String putBlob(String containerName, Blob blob) {
        this.streamletsExecution.interceptAndProcessRequest(containerName, blob, true);
        // Reply to the client once the storage log storage completes, and forward the
        // operation to the next stage.
        logger.info("PUT request/Blob successfully processed.");
        return super.putBlob(containerName, blob);
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
        this.streamletsExecution.interceptAndProcessRequest(containerName, blob, true);
        // Reply to the client once the storage log storage completes, and forward the
        // operation to the next stage.
        logger.info("PUT request/Blob successfully processed.");

        return super.putBlob(containerName, blob, putOptions);
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
    public Blob getBlob(String containerName, String blobName) {
        Blob proxyBlob = super.getBlob(containerName, blobName);
        // Intercepting the blob
        this.streamletsExecution.interceptAndProcessRequest(containerName, proxyBlob, false);
        logger.info("GET request/Blob successfully processed.");

        return proxyBlob;
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        Blob proxyBlob = super.getBlob(containerName, blobName, getOptions);
        // Intercepting the blob
        this.streamletsExecution.interceptAndProcessRequest(containerName, proxyBlob, false);
        logger.info("GET request/Blob successfully processed.");

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
        logger.info("Multipart upload initiated for container: {}", container);
        return super.initiateMultipartUpload(container, blobMetadata, options);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        logger.info("Multipart upload aborted");
        super.abortMultipartUpload(mpu);
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
        logger.info("Multipart upload successfully completed");
        return super.completeMultipartUpload(mpu, parts);
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
        payload = this.streamletsExecution.interceptAndProcessMultipartUpload(mpu, partNumber,
                payload);
        logger.info("Part intercepted and successfully processed");

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
}
