package io.nexus.configuration;

//A class for managing S3Proxy's environment variables and properties 
public class S3ProxyConfig {
    private final static String PROPERTY_NAME = "s3proxy";

    private String identity;
    private String credential;
    private String endpoint;

    public S3ProxyConfig(PropertiesLoader config) {
        this.identity = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "identity");
        this.credential = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "credential");
        this.endpoint = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "endpoint");
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
        return "S3ProxyProperties{" + "identity='" + identity + '\'' + ", credential='" + credential + '\''
                + ", endpoint='" + endpoint + '\'' + '}';
    }

}
