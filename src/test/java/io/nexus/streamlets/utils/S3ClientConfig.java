package io.nexus.streamlets.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3ClientConfig {
    public static AmazonS3 createS3Client() {
        String endpoint = "http://localhost:8181"; // Custom S3 server
        String region = "us-east-1"; // Use any region (not always needed for custom servers)
        String accessKey = "your-access-key";
        String secretKey = "your-secret-key";

        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withPathStyleAccessEnabled(true)  // Important for custom servers (e.g., MinIO)
                .build();
    }
}
