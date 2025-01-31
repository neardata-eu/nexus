package io.nexus.streamlets.metadata;

import java.util.List;

public class StreamletExecutionDescriptor {

    private StreamletDescriptor streamlet;
    private Region region;
    private List<String> arguments;

    public StreamletExecutionDescriptor() {
        // Needed for (de)serialization
    }

    public StreamletExecutionDescriptor(StreamletDescriptor streamlet, Region region, List<String> arguments) {
        this.streamlet = streamlet;
        this.region = region;
        this.arguments = arguments;
    }

    public StreamletDescriptor getStreamlet() {
        return streamlet;
    }

    public Region getRegion() {
        return region;
    }

    public List<String> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return "StreamletExecutionDescriptor{" +
                "streamlet=" + streamlet +
                ", region=" + region +
                ", arguments=" + arguments +
                '}';
    }
}
