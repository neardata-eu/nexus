package io.nexus.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

//A class for managing JClouds' environement variables and proprties 
@ConfigurationProperties("jclouds")
public class JCloudsConfig {

    private String provider;
    private String identity;
    private String credential;
    private String filesystemBasedir;
    private String endpoint;

    public JCloudsConfig() {
    }

    public JCloudsConfig(String provider, String identity, String credential, String filesystemBasedir,
            String endpoint) {
        this.provider = provider;
        this.identity = identity;
        this.credential = credential;
        this.filesystemBasedir = filesystemBasedir;
        this.endpoint = endpoint;
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
        return "JCloudsProperties{" +
                "provider='" + provider + '\'' +
                ", identity='" + identity + '\'' +
                ", credential='" + credential + '\'' +
                ", filesystem_base_directory='" + filesystemBasedir + '\'' +
                ", storage_endpoint='" + endpoint + '\'' +
                '}';
    }

}
