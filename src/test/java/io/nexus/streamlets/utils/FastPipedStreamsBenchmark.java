package io.nexus.streamlets.utils;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FastPipedStreamsBenchmark {

    private static final int DATA_SIZE = 100 * 1024 * 1024; // 100 MB
    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 2 MB
    private static final int CHUNK_SIZE = 64 * 1024; // 64 KB chunks

    public static void main(String[] args) throws InterruptedException {
        int availableProcessors = Runtime.getRuntime().availableProcessors();

        // Warm-up phase to allow JIT optimizations
        System.out.println("Warming up JVM...");
        runBenchmark(1, 2);
        System.gc(); // Reduce GC influence

        System.out.println("\n=== Benchmarking ===");
        for (int streams : new int[]{1, 2, 4, 8, 16}) {
            runBenchmark(streams, availableProcessors);
        }
    }

    private static void runBenchmark(int numStreams, int numThreads) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(numStreams * 2); // One writer & one reader per stream

        FastPipedOutputStream[] outputStreams = new FastPipedOutputStream[numStreams];
        FastPipedInputStream[] inputStreams = new FastPipedInputStream[numStreams];

        for (int i = 0; i < numStreams; i++) {
            outputStreams[i] = new FastPipedOutputStream(BUFFER_SIZE);
            inputStreams[i] = new FastPipedInputStream(outputStreams[i]);
        }

        System.gc(); // Reduce GC effects
        long startTime = System.nanoTime();

        for (int i = 0; i < numStreams; i++) {
            final int index = i;

            // Writer thread
            executor.submit(() -> {
                try {
                    byte[] data = new byte[CHUNK_SIZE];
                    int remaining = DATA_SIZE;
                    while (remaining > 0) {
                        int chunkSize = Math.min(remaining, CHUNK_SIZE);
                        outputStreams[index].write(data, 0, chunkSize);
                        remaining -= chunkSize;
                    }
                    outputStreams[index].close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            // Reader thread
            executor.submit(() -> {
                try {
                    byte[] buffer = new byte[CHUNK_SIZE];
                    while (inputStreams[index].read(buffer) != -1) {
                        // Simulate processing
                    }
                    inputStreams[index].close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        long endTime = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (DATA_SIZE * numStreams) / (durationMs / 1000.0) / (1024.0 * 1024.0); // MB/s

        System.out.printf("Results: %d streams, %d threads -> Time: %d ms, Throughput: %.2f MB/s%n",
                numStreams, numThreads, durationMs, throughput);
    }
}