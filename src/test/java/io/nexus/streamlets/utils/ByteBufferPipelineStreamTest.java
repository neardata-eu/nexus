package io.nexus.streamlets.utils;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ByteArraySegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ByteBufferPipelineStreamTest {

    @Test
    public void testByteArraySegmentReads() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteArraySegment byteArraySegment = new ByteArraySegment(data);
        // Without offsets, both allocated and length are equal.
        Assertions.assertEquals(byteArraySegment.getLength(), data.length);
        Assertions.assertEquals(byteArraySegment.getAllocatedLength(), data.length);

        // Instantiate byteArraySegment with offsets.
        byteArraySegment = new ByteArraySegment(data, 0, 5);
        // Length is the number of bytes in the ByteArraySegment.
        Assertions.assertEquals(byteArraySegment.getLength(), 5);
        // Allocated length retrieves the underlying size of the backing array.
        Assertions.assertEquals(byteArraySegment.getAllocatedLength(), 10);
    }

    @Test
    public void testByteBufferPipelineStream() throws IOException {
        // Check the behavior of ByteBufferPipelineStream with a single segment.
        ByteBufferPipelineStream inputStream = new ByteBufferPipelineStream();
        byte[] data10 = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        Assertions.assertEquals(inputStream.available(), 0);
        inputStream.addSegment(new ByteArraySegment(data10));
        Assertions.assertEquals(inputStream.available(), 10);
        inputStream.close();
        Assertions.assertArrayEquals(data10, inputStream.readAllBytes());

        // Exercise ByteBufferPipelineStream with ByteArraySegment mapping to byte[] slices.
        inputStream = new ByteBufferPipelineStream();
        ByteArraySegment slice = new ByteArraySegment(data10, 0, 5);
        inputStream.addSegment(slice);
        Assertions.assertEquals(inputStream.available(), 5);
        inputStream.close();
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, inputStream.readAllBytes());

        inputStream = new ByteBufferPipelineStream();
        slice = new ByteArraySegment(data10, 5, 5);
        inputStream.addSegment(slice);
        Assertions.assertEquals(inputStream.available(), 5);
        inputStream.close();
        Assertions.assertArrayEquals(new byte[]{5, 6, 7, 8, 9}, inputStream.readAllBytes());
    }

    @Test
    public void testConcurrentReadAndWrite() {
        int numArrays = 10;
        int maxArrayLength = 100;

        // Input data for thread t2.
        ByteBufferPipelineStream inputStream = new ByteBufferPipelineStream();
        // OutputStream written by thread t1 to keep originally written data.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        // InputStream where thread t2 stores the data read from inputStream for validation.
        ByteArrayOutputStream readTestData = new ByteArrayOutputStream();

        // Set up a latch to coordinate threads
        CountDownLatch latch = new CountDownLatch(1);
        // Executor for concurrent tasks
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "test");
        AtomicLong timeInMillis = new AtomicLong();
        AtomicLong writtenBytesOriginal = new AtomicLong();
        AtomicLong writtenBytesAfterRead = new AtomicLong();

        // Writer thread (adds segments to the QueueInputStream)
        CompletableFuture<Void> t1 = CompletableFuture.runAsync(() -> {
            try {
                // Wait for the reader to be ready
                latch.await();

                // Add segments
                int totalWrittenBytes = 0;
                for (int i = 0; i < numArrays; i++) {
                    byte[] data = generateRandomArrayWithRandomLength(maxArrayLength, i);
                    totalWrittenBytes += data.length;
                    inputStream.addSegment(new ByteArraySegment(data));
                    outputStream.write(data);
                }
                writtenBytesOriginal.set(totalWrittenBytes);

                // Close the stream to indicate no more data
                inputStream.close();
                outputStream.close();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }, executor);

        // Reader thread (reads data from the QueueInputStream)
        CompletableFuture<Void> t2 = CompletableFuture.runAsync(() -> {
            try {
                // Notify the writer thread to start writing
                latch.countDown();

                byte[] buffer = new byte[123];
                int bytesRead = 0;

                while (true) {
                    int read = inputStream.read(buffer);
                    if (read == -1)
                        break;
                    if (read > 0)
                        readTestData.write(buffer, 0, read);
                    bytesRead += read;
                }
                writtenBytesAfterRead.set(bytesRead);

                // Validate that the original test data and the read data matches.
                Assertions.assertEquals(writtenBytesOriginal.get(), writtenBytesAfterRead.get());
                Assertions.assertArrayEquals(outputStream.toByteArray(), readTestData.toByteArray());

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);

        t1.join();
        t2.join();

        // Shutdown the executor and wait for completion
        ExecutorServiceHelpers.shutdown(executor);
    }

    public static byte[] generateRandomArrayWithRandomLength(int maxLength, long seed) {
        Random random = new Random(seed);
        // Generate a random length for the array
        int length = random.nextInt(maxLength) + 1; // Ensures length is at least 1
        byte[] byteArray = new byte[length];
        // Fill the array with random bytes
        random.nextBytes(byteArray);
        return byteArray;
    }
}
