package io.nexus.streamlets.metadata;

/**
 * Class to capture the required information to connect to a S3 endpoint.
 */
public class S3StorageConfig {

    private String id;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String container;

    public S3StorageConfig () {

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getContainer() {
        return container;
    }

    public void setContainer(String container) {
        this.container = container;
    }

    @Override
    public String toString() {
        return "S3StorageConfig{" +
                "id='" + id + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", container='" + container + '\'' +
                '}';
    }
}
