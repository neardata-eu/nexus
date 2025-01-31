package io.nexus.streamlets.metadata;

public class StreamletDescriptor {

    public enum ExecuteOn {
        PUT, GET, ALL
    }

    private String id;
    private ExecuteOn executeOn; // The type of request to operate on
    private Hardware hardware; // Streamlet type of operation
    private boolean partitionLocality; // If that streamlet should benefit from stream partition locality

    public StreamletDescriptor() {

    }

    public StreamletDescriptor(String id, ExecuteOn executeOn, Hardware hardware, boolean partitionLocality) {
        this.id = id;
        this.executeOn = executeOn;
        this.hardware = hardware;
        this.partitionLocality = partitionLocality;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public ExecuteOn getExecuteOn() {
        return executeOn;
    }

    public void setExecuteOn(ExecuteOn executeOn) {
        this.executeOn = executeOn;
    }

    public Hardware getHardware() {
        return this.hardware;
    }

    public void setHardware(Hardware hardware) {
        this.hardware = hardware;
    }

    public boolean isPartitionLocality() {
        return partitionLocality;
    }

    public void setPartitionLocality(boolean locality) {
        this.partitionLocality = locality;
    }

    @Override
    public String toString() {
        return "Streamlet{" +
                "id='" + id + '\'' +
                ", executeOn ='" + executeOn + '\'' +
                ", type='" + hardware + '\'' +
                ", partitionLocality=" + (partitionLocality ? "Yes" : "No") +
                '}';
    }
}
