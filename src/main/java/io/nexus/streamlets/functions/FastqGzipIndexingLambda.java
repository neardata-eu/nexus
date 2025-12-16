package io.nexus.streamlets.functions;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class FastqGzipIndexingLambda implements RequestHandler<S3Event, String> {
    private static final int RECORDS_PER_BLOCK = 100000; // Each FASTQ record is 4 lines
    private static final String GZIP_INDEX_METADATA_TAG = "fastqgzip-index";
    private static final int GZIP_COMPRESSION_LEVEL = 1;
    private static final String DESTINATION_BUCKET = "nexus-lambda-events-dest";

    protected void processPutBytes(InputStream inputStream, OutputStream outputStream, String objectKey) {
        try 
        (
                BufferedInputStream in = new BufferedInputStream(inputStream, 64 * 1024);
                BufferedOutputStream out = new BufferedOutputStream(outputStream, 64 * 1024)
        ) 
        {
            List<Long> blockOffsets = fastqGzipIndexing(in, out);
            // List<Long> blockOffsets = fastqGzipIndexing(inputStream, outputStream);
            String compactIndex = blockOffsets.stream().map(String::valueOf).collect(Collectors.joining(","));
            getLogger().info(compactIndex);
            putUserMetadata(GZIP_INDEX_METADATA_TAG, compactIndex, objectKey);
        } catch (IOException e) {
            throw new RuntimeException("FASTQ GZIP indexing failed", e);
        }
    }

    private void putUserMetadata(String gzipIndexMetadataTag, String compactIndex, String objectKey) {
        S3Client s3Client = S3Client.builder().build();
        s3Client.putObjectTagging(builder -> builder
            .bucket(DESTINATION_BUCKET)
            .key(objectKey)
            .tagging(t -> t.tagSet(
                software.amazon.awssdk.services.s3.model.Tag.builder()
                    .key(gzipIndexMetadataTag)
                    .value(compactIndex)
                    .build()
            ))
        );

    }

        @VisibleForTesting
        List<Long> fastqGzipIndexing(InputStream in, OutputStream out) throws IOException {
            List<Long> blockOffsets = new ArrayList<>();
            long currentOffset = 0;

            BufferedReader reader = new BufferedReader(new InputStreamReader(in), 64 * 1024);
            ReusableByteArrayOutputStream blockBuffer = new ReusableByteArrayOutputStream(256 * 1024);
            String line;
            int lineCount = 0;
            int recordCount = 0;
            boolean foundFirstAt = false;

            while ((line = reader.readLine()) != null) {
                byte[] lineBytes = (line + "\n").getBytes(StandardCharsets.UTF_8);

                // getLogger().info("READ LINE: " + line);
                if (!foundFirstAt) {
                    if (line.startsWith("@")) {
                        foundFirstAt = true;
                        lineCount = 1;

                        // compress and write data before first @ (excluding this line)
                        if (blockBuffer.size() > 0) {
                            getLogger().info("COMPRESSING INITIAL DATA " + currentOffset);
                            byte[] compressed = gzipCompress(blockBuffer.getBuffer(), 0, blockBuffer.size());
                            out.write(compressed);
                            currentOffset += compressed.length;
                        }
                        blockBuffer.reset(); // start new block from first @
                    } else {
                        blockBuffer.write(lineBytes); // only write non-record header lines before first @
                        continue;
                    }
                }

                // At this point, foundFirstAt is true, so always write the line
                blockBuffer.write(lineBytes);

                lineCount++;
                if (lineCount == 4) {
                    lineCount = 0;
                    recordCount++;
                    if (recordCount % RECORDS_PER_BLOCK == 0) {
                        getLogger().info("CREATING NEW COMPRESSION BLOCK " + recordCount);
                        long iniTime = System.currentTimeMillis();
                        byte[] compressed = gzipCompress(blockBuffer.getBuffer(), 0, blockBuffer.size());
                        getLogger().info("COMPRESSION TIME " + (System.currentTimeMillis() - iniTime));
                        out.write(compressed);
                        System.out.println("Writting compressed data with size" + compressed.length);
                        blockOffsets.add(currentOffset);
                        currentOffset += compressed.length;
                        blockBuffer.reset();
                    }
                }
            }

            if (blockBuffer.size() > 0) {
                byte[] compressed = gzipCompress(blockBuffer.getBuffer(), 0, blockBuffer.size());
                out.write(compressed);
                if (foundFirstAt) {
                    blockOffsets.add(currentOffset);
                }
                currentOffset += compressed.length;
            }

            out.flush();
            return blockOffsets;
        }

    static class ReusableByteArrayOutputStream extends ByteArrayOutputStream {
        public ReusableByteArrayOutputStream(int size) {
            super(size);
        }

        public byte[] getBuffer() {
            return this.buf;
        }

        public int size() {
            return this.count;
        }
    }

    private static byte[] gzipCompress(byte[] data, int offset, int length) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(length / 2);
        GzipParameters params = new GzipParameters();
        params.setCompressionLevel(GZIP_COMPRESSION_LEVEL); // 1–9
        try (GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(baos, params)) {
            gzip.write(data, offset, length);
        }
        return baos.toByteArray();
    }

    private org.slf4j.Logger getLogger() {
        return org.slf4j.LoggerFactory.getLogger(FastqGzipIndexingLambda.class);
    }

    private String run(String srcBucket, String srcKey) {
        try {

            S3Client s3Client = S3Client.builder().build();
    
            InputStream s3InputStream = s3Client.getObject(builder -> builder.bucket(srcBucket).key(srcKey));
            
            PipedOutputStream outputStream = new PipedOutputStream();
            PipedInputStream inputStream = new PipedInputStream(outputStream, 64 * 1024);
            
            ExecutorService executor = Executors.newFixedThreadPool(4);
    
            executor.submit(()->{
                try {
                    processPutBytes(s3InputStream, outputStream, srcKey);
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            S3AsyncClient s3AsyncClientCrt = S3AsyncClient.crtCreate();
            AsyncRequestBody body = AsyncRequestBody.fromInputStream(inputStream, null, executor);
            CompletableFuture<PutObjectResponse> responseFuture =
                s3AsyncClientCrt.putObject(r -> r.bucket(DESTINATION_BUCKET).key(srcKey), body);
            PutObjectResponse response = responseFuture.join(); // Wait for the response.

            System.out.println("Object " + srcKey + " etag: " + response.eTag());
            System.out.println("Object " + srcKey + " uploaded to bucket " + DESTINATION_BUCKET + ".");

            executor.shutdown();
            s3Client.close();        
        
            return "OK";
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        long startLambdaTime = System.currentTimeMillis();
        S3EventNotificationRecord record = s3event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();
        
        run(srcBucket, srcKey);        
        long endLambdaTime = System.currentTimeMillis();
        long lambdaMemoryMb = context.getMemoryLimitInMB();
        long remainingTimeMs = context.getRemainingTimeInMillis();

        Map<String, Object> resultJson = Map.of("startLambdaTimeMs", startLambdaTime,
                                               "endLambdaTimeMs", endLambdaTime,
                                               "totalLambdaTimeMs", endLambdaTime - startLambdaTime,
                                               "lambdaMemoryMb", lambdaMemoryMb,
                                               "remainingTimeMs", remainingTimeMs);
        Gson gson = new Gson();
        String resultJsonStr = gson.toJson(resultJson);
        getLogger().info("Lambda execution details: " + resultJsonStr); 
        return resultJsonStr;
    }

    public static void main(String[] args) {
        String srcBucket = "nexus-data-3";
        String srcKey = "SRR11478824_R1.fastq";

        FastqGzipIndexingLambda lambda = new FastqGzipIndexingLambda();
        lambda.run(srcBucket, srcKey);

    }


}
