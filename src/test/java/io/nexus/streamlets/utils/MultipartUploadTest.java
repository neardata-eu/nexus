package io.nexus.streamlets.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Test for exercising parallel part uploads in multipart S3 uploads and downloads.
 */
public class MultipartUploadTest {

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {
        int numberOfParts = 1;
        long totalFileSize = 5 * 1024 * 1024; // 50 MB
        long partSize = totalFileSize / numberOfParts;

        String bucketName = "test-metadata";
        String objectKey = "scope3/stream/test.txt";
        File file = new File("/tmp/test.txt");
        File downloadedFile = new File("/tmp/test_downloaded.txt");

        AmazonS3 s3Client = S3ClientConfig.createS3Client();

        generateTextFile(file, totalFileSize);

        // Step 1: Initiate multipart upload
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectKey);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        String uploadId = initResponse.getUploadId();
        System.out.println("Upload ID: " + uploadId);

        // Step 2: Upload parts in parallel
        ExecutorService executor = Executors.newFixedThreadPool(numberOfParts);
        List<CompletableFuture<PartETag>> futures = new ArrayList<>();

        for (int partNumber = 1; partNumber <= numberOfParts; partNumber++) {
            final int currentPart = partNumber;
            CompletableFuture<PartETag> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return uploadPart(s3Client, bucketName, objectKey, file, uploadId, currentPart, partSize);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            futures.add(future);
        }

        // Wait for all parts to upload
        List<PartETag> partETags = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        // Step 3: Complete multipart upload
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, objectKey, uploadId, partETags);
        CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(completeRequest);
        System.out.println("Upload complete: " + result.getLocation());

        // Shutdown executor
        executor.shutdown();

        System.err.println("WAITING BEFORE DOWNLOAD ");
        Thread.sleep(5000);

        // Step 4: Download the uploaded file
        downloadFile(s3Client, bucketName, objectKey, downloadedFile);

        // Step 5: Compute and compare checksums
        String uploadedChecksum = computeSHA256Checksum(file);
        String downloadedChecksum = computeSHA256Checksum(downloadedFile);

        System.out.println("Uploaded File Checksum  : " + uploadedChecksum);
        System.out.println("Downloaded File Checksum: " + downloadedChecksum);

        if (uploadedChecksum.equals(downloadedChecksum)) {
            System.out.println("✅ File verification successful! Checksums match.");
        } else {
            System.err.println("❌ File verification failed! Checksums do not match.");
        }
    }

    private static PartETag uploadPart(AmazonS3 s3Client, String bucketName, String objectKey, File file, String uploadId, int partNumber, long partSize) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] buffer = new byte[(int) partSize];
            raf.seek((partNumber - 1) * partSize);
            int bytesRead = raf.read(buffer);

            System.out.println("Uploading part " + partNumber + " (" + bytesRead + " bytes)");

            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(objectKey)
                    .withUploadId(uploadId)
                    .withPartNumber(partNumber)
                    .withInputStream(new ByteArrayInputStream(buffer, 0, bytesRead))
                    .withPartSize(bytesRead);

            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
            return uploadResult.getPartETag();
        }
    }

    private static void generateRandomFile(File file, long fileSize) throws IOException {
        System.out.println("Generating random file: " + file.getAbsolutePath() + " (" + fileSize + " bytes)");
        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] buffer = new byte[1024 * 1024]; // 1MB buffer
            Random random = new Random();
            long remaining = fileSize;
            while (remaining > 0) {
                int chunkSize = (int) Math.min(buffer.length, remaining);
                random.nextBytes(buffer);
                fos.write(buffer, 0, chunkSize);
                remaining -= chunkSize;
            }
        }
        System.out.println("File generation complete.");
    }

    private static void generateTextFile(File file, long fileSize) throws IOException {
        System.out.println("Generating text file: " + file.getAbsolutePath() + " (" + fileSize + " bytes)");

        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
        Random random = new Random();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            long remaining = fileSize;
            while (remaining > 0) {
                int chunkSize = (int) Math.min(1024, remaining); // Write in 1KB chunks
                StringBuilder sb = new StringBuilder(chunkSize);

                for (int i = 0; i < chunkSize; i++) {
                    sb.append(characters.charAt(random.nextInt(characters.length())));
                }

                writer.write(sb.toString());
                remaining -= chunkSize;
            }
        }

        System.out.println("Text file generation complete.");
    }

    private static void downloadFile(AmazonS3 s3Client, String bucketName, String objectKey, File outputFile) throws IOException {
        System.out.println("Downloading file...");
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
        try (InputStream inputStream = s3Object.getObjectContent();
             FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            byte[] buffer = new byte[16 * 1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
        System.out.println("Download complete.");
    }

    private static String computeSHA256Checksum(File file) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[16 * 1024];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        byte[] hash = digest.digest();
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }
}
