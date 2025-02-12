package io.nexus.streamlets.utils;

import java.io.*;
import java.util.concurrent.CountDownLatch;

public class FastPipedStreamsBenchmark {

    public static void main(String[] args) throws IOException, InterruptedException {
        final int BUFFER_SIZE = 16 * 1024; // 16 KB buffer
        final int TOTAL_DATA_SIZE = 1 * 1024 * 1024 * 1024; // 1 GB
        final byte[] writeBuffer = new byte[BUFFER_SIZE];

        FastPipedOutputStream fastOut = new FastPipedOutputStream(BUFFER_SIZE);
        FastPipedInputStream fastIn = new FastPipedInputStream(fastOut);

        CountDownLatch latch = new CountDownLatch(2);

        // Writer Thread
        Thread writerThread = new Thread(() -> {
            try {
                long startTime = System.nanoTime();
                int bytesWritten = 0;
                while (bytesWritten < TOTAL_DATA_SIZE) {
                    int toWrite = Math.min(writeBuffer.length, TOTAL_DATA_SIZE - bytesWritten);
                    fastOut.write(writeBuffer, 0, toWrite);
                    bytesWritten += toWrite;
                }
                fastOut.close();
                long endTime = System.nanoTime();
                double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
                double mbps = (TOTAL_DATA_SIZE / (1024.0 * 1024.0)) / durationSeconds;
                System.out.printf("Writer Throughput: %.2f MB/s%n", mbps);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        });

        // Reader Thread
        Thread readerThread = new Thread(() -> {
            try {
                long startTime = System.nanoTime();
                byte[] readBuffer = new byte[BUFFER_SIZE];
                int bytesRead = 0;
                int n;
                while ((n = fastIn.read(readBuffer)) != -1) {
                    bytesRead += n;
                }
                long endTime = System.nanoTime();
                double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
                double mbps = (TOTAL_DATA_SIZE / (1024.0 * 1024.0)) / durationSeconds;
                System.out.printf("Reader Throughput: %.2f MB/s%n", mbps);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        });

        writerThread.start();
        readerThread.start();

        latch.await(); // Wait for both threads to finish
    }
}