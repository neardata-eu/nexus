package io.nexus.streamlets.metadata;

public class StreamletDescriptor {
    public enum ExecuteOn {
        PUT, GET, ALL
    }

    public enum Type {
        TRANSFORMER, PERFORMANCE, ROUTING, SEMANTIC
    }

    public enum ResourceUsage {
        CPU, IO
    }

    private String id;
    private ExecuteOn executeOn; // The type of request to operate on
    private Type type; // Streamlet type of operation
    private boolean partitionLocality; // If that streamlet should benefit from stream partition locality
    private ResourceUsage resourceUsage; // Exepected resource to use
    private boolean requiresGPU; // If the operation requires any GPU usage

    public StreamletDescriptor() {

    }

    public StreamletDescriptor(String id, ExecuteOn executeOn, Type type, boolean partitionLocality,
            ResourceUsage resourceUsage, boolean requiresGPU) {
        this.id = id;
        this.executeOn = executeOn;
        this.type = type;
        this.partitionLocality = partitionLocality;
        this.resourceUsage = resourceUsage;
        this.requiresGPU = requiresGPU;
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

    public Type getType() {
        return type;
    }

    public void setType(Type streamletType) {
        this.type = streamletType;
    }

    public boolean isPartitionLocality() {
        return partitionLocality;
    }

    public void setPartitionLocality(boolean locality) {
        this.partitionLocality = locality;
    }

    public ResourceUsage getResourceUsage() {
        return resourceUsage;
    }

    public void setResourceUsage(ResourceUsage resourceUsage) {
        this.resourceUsage = resourceUsage;
    }

    public Boolean requiresGPU() {
        return requiresGPU;
    }

    public void setRequiresGPU(boolean requiresGPU) {
        this.requiresGPU = requiresGPU;
    }

    @Override
    public String toString() {
        return "Streamlet{" +
                "id='" + id + '\'' +
                ", executeOn ='" + executeOn + '\'' +
                ", type='" + type + '\'' +
                ", partitionLocality=" + (partitionLocality ? "Yes" : "No") +
                ", resourceUsage='" + resourceUsage + '\'' +
                ", requiresGPU=" + (requiresGPU ? "Yes" : "No") +
                '}';
    }
}
