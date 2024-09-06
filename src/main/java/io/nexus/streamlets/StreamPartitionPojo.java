package io.nexus.streamlets;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamPartitionPojo {

    public final static Pattern KAFKA_PARTITION_OBJECT_PATTERN =
            Pattern.compile("^([a-zA-Z0-9_-]+)/([a-zA-Z0-9_-]+)/(\\d+)/(\\d{20}-[a-zA-Z0-9_-]+\\.[a-zA-Z0-9_-]+)$");

    public final String container;
    public final String scope;
    public final String stream;
    public final String partition;
    public final String object;

    public StreamPartitionPojo(String containerName, String scopeName, String streamName,
                               String partitionName, String objectName) {
        this.container = containerName;
        this.scope = scopeName;
        this.stream = streamName;
        this.partition = partitionName;
        this.object = objectName;
    }

    @Override
    public String toString() {
        return "StreamPartitionPojo{" +
                "container='" + container + '\'' +
                ", scope='" + scope + '\'' +
                ", stream='" + stream + '\'' +
                ", partition='" + partition + '\'' +
                ", object='" + object + '\'' +
                '}';
    }

    public static StreamPartitionPojo buildStreamPartitionPojoFromKafkaRequestPath(String fullyQualifiedKafkaRequestPath) {
        Matcher matcher = KAFKA_PARTITION_OBJECT_PATTERN.matcher(fullyQualifiedKafkaRequestPath);
        if (matcher.matches()) {
            StreamPartitionPojo pojo = new StreamPartitionPojo("test-bucket", matcher.group(1),
                    matcher.group(2), matcher.group(3), matcher.group(4));
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
