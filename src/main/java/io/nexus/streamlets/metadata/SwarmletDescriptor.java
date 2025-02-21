package io.nexus.streamlets.metadata;

/**
 * Metadata descriptor for a swarmlet (i.e., group of identical Nexus
 * worker instances running as a unified service). The descriptor of
 * the Swarmlet captures the endpoint of the service, the "region" where
 * the swarmlet is located and the specialized "hardware" available (e.g.,
 * GPU). This information is useful for taking routing decisions.
 */
public class SwarmletDescriptor {

    private String serviceEndpoint;
    private Region region;
    private Hardware hardware;

    public SwarmletDescriptor() {

    }
    public SwarmletDescriptor(String serviceEndpoint, Region region, Hardware hardware) {
        this.serviceEndpoint = serviceEndpoint;
        this.region = region;
        this.hardware = hardware;
    }

    public String getServiceEndpoint() {
        return serviceEndpoint;
    }

    public void setServiceEndpoint(String serviceEndpoint) {
        this.serviceEndpoint = serviceEndpoint;
    }

    public Region getRegion() {
        return this.region;
    }

    public void setRegion(Region region) {
        this.region = region;
    }

    public Hardware getHardware() {
        return this.hardware;
    }

    public void setHardware(Hardware hardware) {
        this.hardware = hardware;
    }

    @Override
    public String toString() {
        return "SwarmletDescriptor{" +
                "serviceEndpoint='" + serviceEndpoint + '\'' +
                ", region=" + region +
                ", hardware=" + hardware +
                '}';
    }
}
