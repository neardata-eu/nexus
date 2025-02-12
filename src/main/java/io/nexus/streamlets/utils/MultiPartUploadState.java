package io.nexus.streamlets.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** 
 * Class to track the status of a Multipart upload in the process of transforming it into a regular PUT request.
 */
public class MultiPartUploadState {

    private final String container;
    private final BlobMetadata blobMetadata;
    private final PutOptions options;
    private final String uploadId;

    private final OutputStream outputStream; // Output stream to write the content of parts
    private final InputStream inputStream; // Input stream for the new PUT request with the contents of parts.
    private final AtomicBoolean uploadCompleted;
    private final AtomicBoolean uploadInitialized;
    private CompletableFuture<Void> putRequest;

    // Keep the individual metadata and content of parts in sorted order by partNumber.
    private final Map<Integer, MultipartPartState> uploadParts = new TreeMap<>();

    public MultiPartUploadState(String container, BlobMetadata blobMetadata, PutOptions options, String uploadId) {
        this.container = container;
        this.blobMetadata = blobMetadata;
        this.options = options;
        this.uploadId = uploadId;
        this.uploadCompleted = new AtomicBoolean(false);
        this.uploadInitialized = new AtomicBoolean(false);
        // Initialize streams for transferring the multipart upload data to a regular PUT.
        this.outputStream = new FastPipedOutputStream(); // Need a larger buffer to prevent blocking writes
        this.inputStream = new FastPipedInputStream((FastPipedOutputStream) this.outputStream);
    }

    /**
     * Buffers the part data and tracks it in the uploaded parts list. Note that we assume that parts for the same
     * partition log file may be uploaded in parallel. This means that we cannot rely on the order of uploaded parts
     * from the client perspective. For this reason, we have to buffer all the parts (e.g., in memory) and then stream
     * all the sorted contents as a single PUT request.
     */
    public Long uploadPart(int partNumber, Payload payload) {
        // Use an expandable buffer since we don't know the size beforehand
        ByteBuf buffer = Unpooled.buffer();
        byte[] tempBuffer = new byte[8192];
        int bytesRead;
        long transferredBytes = 0L;
        try {
            while ((bytesRead = payload.openStream().read(tempBuffer)) != -1) {
                buffer.writeBytes(tempBuffer, 0, bytesRead);
                transferredBytes += bytesRead;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        // We keep parts of the multipart upload temporarily in off-heap memory until we can assemble them.
        addPart(partNumber, transferredBytes, buffer);
        return transferredBytes;
    }

    /**
     * Adds a part to the uploaded parts list.
     */
    private void addPart(int partNumber, long size, ByteBuf content) {
        this.uploadParts.put(partNumber, new MultipartPartState(
                MultipartPart.create(partNumber, size, null, new Date()), content));
    }

    /**
     * Transfers the content of all the parts to the internal output stream.
     */
    public void transferMultiPartContentsToPutRequest() {
        try {
            for (MultipartPartState mps : this.uploadParts.values()) {
                byte[] bytes = new byte[mps.partContent.readableBytes()];
                mps.partContent.readBytes(bytes);
                this.outputStream.write(bytes);
            }
            this.outputStream.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }  finally {
            // Release the buffers to prevent memory leaks.
            this.uploadParts.values().forEach(mps -> mps.partContent.release());
        }
    }

    /**
     * Completes the upload by closing the output stream.
     */
    public void completeUpload() {
        if (uploadCompleted.compareAndSet(false, true)) {
            try {
                // Close the output stream that is feeding Streamlets, so we can release them from reading.
                this.outputStream.close();
                // Join the PUT request initiated at the beginning of the multipart upload. If it has failed, this
                // should throw an exception.
                this.putRequest.join();
            } catch (IOException e) {
                throw new RuntimeException("Error closing output stream", e);
            }
        }
    }

    /**
     * Aborts the upload.
     */
    public void abortUpload() {
        if (uploadCompleted.compareAndSet(false, true)) {
            try {
                // Close the output stream as the upload has been aborted.
                outputStream.close();
                // Release buffers.
                this.uploadParts.values().forEach(mps -> mps.partContent.release());
                // Complete the PUT request exceptionally.
                this.putRequest.completeExceptionally(new IllegalStateException("Multipart upload aborted."));
            } catch (IOException e) {
                throw new RuntimeException("Error closing output stream", e);
            }
        }
    }

    public AtomicBoolean isUploadInitialized() {
        return this.uploadInitialized;
    }

    public void setPutRequest(CompletableFuture<Void> putRequest) {
        this.putRequest = putRequest;
    }

    public CompletableFuture<Void> getPutRequest() {
        return putRequest;
    }

    public List<MultipartPart> listParts() {
        List<MultipartPart> result;
        synchronized (this.uploadParts) {
            result = this.uploadParts.values().stream().map(mps -> mps.multipartPart).toList();
        }
        return result;
    }

    public BlobMetadata getBlobMetadata() {
        return blobMetadata;
    }

    public String getContainer() {
        return container;
    }

    public PutOptions getOptions() {
        return options;
    }

    public String getUploadId() {
        return uploadId;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public AtomicBoolean getUploadCompleted() {
        return uploadCompleted;
    }

    public long getMultipartUploadSize() {
        return this.uploadParts.values().stream()
                .map(mps -> mps.multipartPart.partSize())
                .reduce(0L ,Long::sum);
    }

    /**
     * Private class to wrap a MultiPart and manage its state to handle parallel upload parts.
     */
    class MultipartPartState {
        private MultipartPart multipartPart;
        private ByteBuf partContent;

        public MultipartPartState(MultipartPart multipartPart, ByteBuf partContent) {
            this.multipartPart = multipartPart;
            this.partContent = partContent;
        }
    }
}
