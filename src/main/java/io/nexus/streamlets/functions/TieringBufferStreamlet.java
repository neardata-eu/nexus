package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.DataSourceStreamlet;
import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TieringBufferStreamlet extends ByteStreamlet implements DataSourceStreamlet {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String name = "BUFFERING_STREAMLET";
    private static final Path LOCAL_CACHE_DIR = Paths.get("/tmp/tiering-buffer");
    private static final int RETRY_INTERVAL_MS = 5000;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;

    // Use LinkedBlockingQueue for blocking operations
    private final LinkedBlockingQueue<RetryEntry> retryQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread t = new Thread(r, "TieringBuffer-RetryThread");
                t.setDaemon(true);
                return t;
            }
    );
    private volatile boolean shutdown = false;

    private record RetryEntry(Path file, StreamletContext context, int attemptCount, long firstAttemptTime) {
        public RetryEntry(Path file, StreamletContext context) {
            this(file, context, 1, System.currentTimeMillis());
        }

        public RetryEntry withIncrementedAttempt() {
            return new RetryEntry(file, context, attemptCount + 1, firstAttemptTime);
        }
    }

    public TieringBufferStreamlet() {
        try {
            Files.createDirectories(LOCAL_CACHE_DIR);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create local cache directory", e);
        }

        // Start the retry loop immediately
        retryScheduler.execute(this::retryLoop);
    }

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        String objectId = context.getStreamPartition().getScopedPartitionUri();
        Path localFile = LOCAL_CACHE_DIR.resolve(objectId);

        try {
            // Create the directory for the buffer file
            Files.createDirectories(localFile.getParent());
        } catch (IOException e) {
            logger.error("Failed to create directory for: {}", localFile, e);
            throw new RuntimeException("Failed to create directory for: " + localFile, e);
        }

        try (OutputStream localOut = Files.newOutputStream(localFile,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             InputStream inputStream = dataStreams.input();
             OutputStream outputStream = dataStreams.output()) {

            // Buffer data waiting for the previous streamlets to complete
            int bytesWritten = doProcess(inputStream, localOut, name, logger);
            context.getLogger().info("Written {} bytes locally to {}", bytesWritten, localFile);

        } catch (IOException e) {
            logger.error("Failed to write to local buffer: {}", localFile, e);
            throw new RuntimeException("Failed to write to local buffer", e);
        }

        // Try immediate upload, queue for retry if it fails
        try {
            retryUpload(localFile, context);
        } catch (Exception e) {
            logger.warn("Initial upload failed for {}, adding to retry queue", localFile, e.getCause());
            retryQueue.offer(new RetryEntry(localFile, context));
        }
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        throw new UnsupportedOperationException("Streamlet " + name + " is not supposed to implement GET processing.");
    }

    @Override
    public InputStream handlePreGet(StreamPartition streamPartition, StreamletContext context) {
        Path localFile = LOCAL_CACHE_DIR.resolve(streamPartition.getScopedPartitionUri());
        if (Files.exists(localFile)) {
            try {
                return Files.newInputStream(localFile);
            } catch (IOException e) {
                context.getLogger().info("File {} not found locally, triggering GET to storage", localFile);
            }
        }
        // Otherwise let the GET continue as usual
        return context.fetchObjectFromPolicyStorage(context.getS3StorageConfigs().getFirst(), streamPartition);
    }

    private void retryUpload(Path localFile, StreamletContext context) throws Exception {
        try (InputStream in = Files.newInputStream(localFile)) {
            context.routeObjectToPolicyStorage(context.getS3StorageConfigs().getFirst(), in, in.available());
            Files.deleteIfExists(localFile); // Use deleteIfExists to avoid exception if file doesn't exist
            context.getLogger().info("Successfully uploaded and cleaned up local file: {}", localFile);
        } catch (Exception e) {
            context.getLogger().warn("Failed to upload file: {}", localFile, e.getCause());
            throw e; // Re-throw to trigger retry logic
        }
    }

    private void retryLoop() {
        while (!shutdown) {
            try {
                // Block until an entry is available or timeout occurs
                RetryEntry entry = retryQueue.poll(RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (entry == null) {
                    // Timeout occurred, continue to check shutdown status
                    continue;
                }

                // Process entries one by one to maintain order and handle storage failures
                boolean success = processRetryEntry(entry);
                if (success) {
                    // Success - continue processing next entry
                    logger.info("Successfully processed retry entry: {}", entry.file());
                } else {
                    // Failed - put back at the front of the queue to maintain order
                    // and pause processing to avoid overwhelming failed storage
                    RetryEntry updatedEntry = entry.withIncrementedAttempt();
                    // Put failed entry back at the front of the queue
                    // This maintains order and prevents processing subsequent entries
                    // when storage is failing
                    retryQueue.offer(updatedEntry);
                    logger.warn("Storage upload failed for entry (attempt {}): {}. " +
                                    "Pausing retry processing to avoid overwhelming storage. " +
                                    "Queue size: {}",
                            updatedEntry.attemptCount(), updatedEntry.file(), retryQueue.size());
                    // Wait longer before retrying when storage is failing
                    Thread.sleep(RETRY_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Retry loop interrupted, shutting down");
                break;
            } catch (Exception e) {
                logger.error("Unexpected error in retry loop", e);
                // Continue processing to avoid stopping the retry mechanism
                try {
                    Thread.sleep(1000); // Brief pause before continuing
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private boolean processRetryEntry(RetryEntry entry) {
        try {
            if (Files.exists(entry.file())) {
                retryUpload(entry.file(), entry.context());
                return true;
            } else {
                logger.warn("Retry entry file no longer exists: {}", entry.file());
                return true; // Consider this "successful" since there's nothing to upload
            }
        } catch (Exception e) {
            logger.debug("Retry attempt failed for: {}", entry.file(), e);
            return false;
        }
    }

    private boolean shouldRetry(RetryEntry entry) {
        // Never give up on entries to prevent data corruption
        // Log warnings for entries that have been retrying for a long time
        long age = System.currentTimeMillis() - entry.firstAttemptTime();
        if (age > TimeUnit.HOURS.toMillis(1) && entry.attemptCount() % 10 == 0) {
            logger.warn("Entry has been retrying for {} hours with {} attempts: {}",
                    TimeUnit.MILLISECONDS.toHours(age), entry.attemptCount(), entry.file());
        }
        return true; // Always retry, never give up
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down TieringBufferStreamlet");
        shutdown = true;

        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.warn("Retry scheduler did not terminate gracefully, forcing shutdown");
                retryScheduler.shutdownNow();

                // Wait a bit more for tasks to respond to interruption
                if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.error("Retry scheduler did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for retry scheduler shutdown");
            retryScheduler.shutdownNow();
        }

        // Log remaining queue size for monitoring
        int remainingEntries = retryQueue.size();
        if (remainingEntries > 0) {
            logger.warn("Shutdown with {} entries remaining in retry queue", remainingEntries);
        }
    }
}