package io.nexus.configuration;

//A class for managing JClouds' environment variables and properties 
public class JCloudsConfig {
    private final static String PROPERTY_NAME = "jclouds";

    private String provider;
    private String identity;
    private String credential;
    private String filesystemBasedir;
    private String endpoint;

    public JCloudsConfig(PropertiesLoader config) {
        this.provider = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "provider");
        this.identity = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "identity");
        this.credential = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "credential");
        this.filesystemBasedir = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "filesystembasedir");
        this.endpoint = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "endpoint");
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
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

    public String getFilesystemBasedir() {
        return filesystemBasedir;
    }

    public void setFilesystemBasedir(String filesystemBasedir) {
        this.filesystemBasedir = filesystemBasedir;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String toString() {
        return "JCloudsProperties{" + "provider='" + provider + '\'' + ", identity='" + identity + '\''
                + ", credential='" + credential + '\'' + ", filesystem_base_directory='" + filesystemBasedir + '\''
                + ", storage_endpoint='" + endpoint + '\'' + '}';
    }

}
