package io.nexus.streamlets;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamPartitionPojo {

    public final static Pattern DEFAULT_PARTITION_OBJECT_PATTERN = Pattern
            .compile("^([a-zA-Z0-9_-]+)/([a-zA-Z0-9_-]+)/([a-zA-Z0-9_-]+\\.[a-zA-Z0-9_-]+)$");

    // Kafka's pattern follows the default pattern, up to three directories, with
    // .log object
    public final static Pattern KAFKA_PARTITION_OBJECT_PATTERN = Pattern
            .compile("^([a-zA-Z0-9_-]+)/([a-zA-Z0-9_-]+)/([a-zA-Z0-9_-]+\\.log)$");

    public final static Pattern PULSAR_PARTITION_OBJECT_PATTERN = Pattern
            .compile("^([a-zA-Z0-9]+)-([a-zA-Z0-9]+)-([a-zA-Z0-9]+)-([a-zA-Z0-9]+)-([a-zA-Z0-9]+)-ledger-([0-9]+)$");

    public final String container;
    public final String scope;
    public final String stream;
    public final String partition;
    public final String object;

    public StreamPartitionPojo(String containerName, String scopeName, String streamName, String partitionName,
            String objectName) {
        this.container = containerName;
        this.scope = scopeName;
        this.stream = streamName;
        this.partition = partitionName;
        this.object = objectName;
    }

    @Override
    public String toString() {
        return "StreamPartitionPojo{" + "container='" + container + '\'' + ", scope='" + scope + '\'' + ", stream='"
                + stream + '\'' + ", partition='" + partition + '\'' + ", object='" + object + '\'' + '}';
    }

    public static StreamPartitionPojo getStreamPartitionPojo(String objectPath, String streamingSystem,
            String container) {
        return switch (streamingSystem) {
            case "kafka" -> buildStreamPartitionPojoFromKafkaRequestPath(objectPath, container);
            case "pulsar" -> buildStreamPartitionPojoFromPulsarRequestPath(objectPath, container);
            default ->
                // This is useful for testing without having to run a streaming system.
                    buildDefaultStreamPartitionPojoFromRequestPath(objectPath, container);
        };
    }

    public static StreamPartitionPojo buildStreamPartitionPojoFromKafkaRequestPath(
            String fullyQualifiedKafkaRequestPath, String container) {
        Matcher matcher = KAFKA_PARTITION_OBJECT_PATTERN.matcher(fullyQualifiedKafkaRequestPath);
        if (matcher.matches()) {

            return new StreamPartitionPojo(container, matcher.group(1), matcher.group(2),
                    matcher.group(2), matcher.group(3));
        }

        // The object doesn't match the pattern.
        throw new MalformedStreamStorageRequestException("Kafka request not matching pattern.");
    }

    public static StreamPartitionPojo buildStreamPartitionPojoFromPulsarRequestPath(
            String fullyQualifiedPulsarRequestPath, String container) {
        Matcher matcher = KAFKA_PARTITION_OBJECT_PATTERN.matcher(fullyQualifiedPulsarRequestPath);
        if (matcher.matches()) {
            // Since Pulsar does not have a stream/scope identifier, it is expected to have
            // a global Pulsar policy for the time being.
            return new StreamPartitionPojo(container, "pulsar", "pulsar", "0",
                    fullyQualifiedPulsarRequestPath);
        }

        // The object doesn't match the pattern.
        throw new MalformedStreamStorageRequestException("Pulsar request not matching pattern.");
    }

    public static StreamPartitionPojo buildDefaultStreamPartitionPojoFromRequestPath(String fullyQualifiedRequestPath,
            String container) {

        Matcher matcher = DEFAULT_PARTITION_OBJECT_PATTERN.matcher(fullyQualifiedRequestPath);
        if (matcher.matches()) {
            StreamPartitionPojo pojo = new StreamPartitionPojo(container, container, matcher.group(1), matcher.group(2),
                    matcher.group(3));
            System.err.println(pojo);
            return pojo;
        }

        // The object doesn't match the pattern.
        return null;
    }

    public String getScopedPartitionUri() {
        return this.scope + File.separator + this.stream + File.separator + this.partition;
    }

    public String getScopedObjectName() {
        return this.getScopedPartitionUri() + File.separator + this.object;
    }

    public String getContainer() {
        return container;
    }

    public String getScope() {
        return scope;
    }

    public String getStream() {
        return stream;
    }

    public String getPartition() {
        return partition;
    }

    public String getObject() {
        return object;
    }
}
