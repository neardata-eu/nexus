package io.nexus.configuration;

import io.nexus.streamlets.metadata.Hardware;
import io.nexus.streamlets.metadata.Region;

public class NexusConfig {

    private final static String PROPERTY_NAME = "nexus";

    private Region region;
    private Hardware hardware;

    public NexusConfig(PropertiesLoader config) {
        this.region = Region.valueOf(config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "region"));
        this.hardware = Hardware.valueOf(config.getString(PROPERTY_NAME + PropertiesLoader.SEPARATOR + "hardware"));
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

    @Override
    public String toString() {
        return "NexusConfig{" +
                "region=" + region +
                ", hardware=" + hardware +
                '}';
    }
}
