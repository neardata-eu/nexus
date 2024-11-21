package io.nexus.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

//A class for managing Redis's environement variables and proprties 
@ConfigurationProperties("redis")
public class RedisConfig {

    private String host;
    private int port;

    public RedisConfig() {
    }

    public RedisConfig(String host, int port) {
        this.host = host;
        this.port = port;
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
        return "RedisConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

}
