package io.nexus.configuration;

import io.nexus.streamlets.metadata.Hardware;
import io.nexus.streamlets.metadata.Region;

public class NexusConfig {

    private final static String PROPERTY_NAME = "nexus";

    private Region region;
    private Hardware hardware;
    private String swarmletName;
    private int clusterVirtualNodes;
    private int keepaliveInterval;
    private int timeout;
    private String stateBackendType;
    private String redisStateBackendHost;
    private int redisStateBackendPort;
    private String rocksDBStateBackendPath;

    public NexusConfig(PropertiesLoader config) {
        // Swarmlet properties
        this.region = Region.valueOf(config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "region"));
        this.hardware = Hardware.valueOf(config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "hardware"));
        this.swarmletName = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "swarmlet");
        // Cluster properties
        this.clusterVirtualNodes = config.getInt(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "clusterVirtualNodes");
        this.keepaliveInterval = config.getInt(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "keepaliveInterval");
        this.timeout = config.getInt(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "timeout");
        // State backend properties
        this.stateBackendType = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "stateBackendType");
        this.redisStateBackendHost = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "redisStateBackendHost");
        this.redisStateBackendPort = config.getInt(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "redisStateBackendPort");
        this.rocksDBStateBackendPath = config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "rocksDBStateBackendPath");
    }

    public Region getRegion() {
        return region;
    }

    public void setRegion(Region region) {
        this.region = region;
    }

    public Hardware getHardware() {
        return hardware;
    }

    public void setHardware(Hardware hardware) {
        this.hardware = hardware;
    }

    public int getClusterVirtualNodes() {
        return clusterVirtualNodes;
    }

    public void setClusterVirtualNodes(int clusterVirtualNodes) {
        this.clusterVirtualNodes = clusterVirtualNodes;
    }

    public String getStateBackendType() {
        return stateBackendType;
    }

    public void setStateBackendType(String stateBackendType) {
        this.stateBackendType = stateBackendType;
    }

    public String getRedisStateBackendHost() {
        return redisStateBackendHost;
    }

    public void setRedisStateBackendHost(String redisStateBackendHost) {
        this.redisStateBackendHost = redisStateBackendHost;
    }

    public int getRedisStateBackendPort() {
        return redisStateBackendPort;
    }

    public void setRedisStateBackendPort(int redisStateBackendPort) {
        this.redisStateBackendPort = redisStateBackendPort;
    }

    public String getRocksDBStateBackendPath() {
        return rocksDBStateBackendPath;
    }

    public void setRocksDBStateBackendPath(String rocksDBStateBackendPath) {
        this.rocksDBStateBackendPath = rocksDBStateBackendPath;
    }

    public int getKeepaliveInterval() {
        return keepaliveInterval;
    }

    public void setKeepaliveInterval(int keepaliveInterval) {
        this.keepaliveInterval = keepaliveInterval;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getClusterRingId() {
        return this.swarmletName + "-" + this.region.name();
    }

    @Override
    public String toString() {
        return "NexusConfig{" +
                "region=" + region +
                ", hardware=" + hardware +
                ", clusterVirtualNodes=" + clusterVirtualNodes +
                ", keepaliveInterval=" + keepaliveInterval +
                ", timeout=" + timeout +
                ", stateBackendType='" + stateBackendType + '\'' +
                ", redisStateBackendHost='" + redisStateBackendHost + '\'' +
                ", redisStateBackendPort=" + redisStateBackendPort +
                ", rocksDBStateBackendPath='" + rocksDBStateBackendPath + '\'' +
                '}';
    }
}
