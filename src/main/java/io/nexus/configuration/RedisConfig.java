package io.nexus.configuration;

//A class for managing Redis's environment variables and properties 
public class RedisConfig {
    private final static String PROPERTY_NAME = "redis";

    private String host;
    private int port;

    public RedisConfig(PropertiesLoader config) {
        this.host = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "host");
        this.port = config.getInt(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "port");
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "RedisConfig{" + "host='" + host + '\'' + ", port=" + port + '}';
    }

}
