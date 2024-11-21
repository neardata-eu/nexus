package io.nexus.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

//A class for managing S3Proxy's environement variables and proprties 
@ConfigurationProperties("s3proxy")
public class S3ProxyConfig {

    private String identity;
    private String credential;
    private String endpoint;

    public S3ProxyConfig() {
    }

    public S3ProxyConfig(String identity, String credential, String endpoint) {
        this.identity = identity;
        this.credential = credential;
        this.endpoint = endpoint;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getCredential() {
        return credential;
    }

    public void setCredential(String credential) {
        this.credential = credential;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String toString() {
        return "S3ProxyProperties{" +
                "identity='" + identity + '\'' +
                ", credential='" + credential + '\'' +
                ", endpoint='" + endpoint + '\'' +
                '}';
    }

}
