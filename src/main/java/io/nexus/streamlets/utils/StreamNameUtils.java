package io.nexus.streamlets.utils;

import java.util.regex.Matcher;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MultipartUpload;

import io.nexus.streamlets.StreamPartitionPojo;

/**
 * Stream names are checked against each system's naming convention to affirm
 * the current system and extract the needed metadata for Policy retrieval
 * 
 * For Kafka: <topicName>-<topicId>/<partitionNumber>/<logFile>. Example:
 * topic1-NocMJpqTTtyKaFJDURStjg/0/00000000000000000000-zsj9DCIER9OaE0B3ZwcMpQ.log
 * 
 * For Pulsar: <ledgerFile>. Example:
 * d536f64d-4a32-43e5-944e-b32e05e3c790-ledger-16
 * 
 * For Filesystem: <scope>/<stream>/<file>, Example: scope/stream/test.txt
 */
public class StreamNameUtils {

    public static final String DEFAULT_STREAM_SEPARATOR = "/";
    public static final String KAFKA_TOPIC_SEPARATOR = "-";

    public enum Frameworks {
        KAFKA, PULSAR, DEFAULT
    }

    public static String getScopeFromRequest(MultipartUpload multipartUpload) {
        return getScopeFromChunkName(multipartUpload.blobName());
    }

    public static String getStreamFromRequest(MultipartUpload multipartUpload) {
        return getStreamFromChunkName(multipartUpload.blobName());

    }

    public static String getStreamFromRequest(Blob blob) {
        return getStreamFromChunkName(blob.getMetadata().getName());
    }

    public static String getScopeFromRequest(Blob blob) {
        return getScopeFromChunkName(blob.getMetadata().getName());
    }

    public static String getScopeFromChunkName(String chunkName) {
        return getChunkNameComponent(chunkName, 0);
    }

    public static String getStreamFromChunkName(String chunkName) {
        return getChunkNameComponent(chunkName, 1);
    }

    private static String getChunkNameComponent(String chunkName, int index) {

        if (chunkName == null || chunkName.isEmpty()) {
            return null;
        }

        Frameworks system = getSystemFromChunk(chunkName);

        String[] nameComponents;

        switch (system) {
        // Kafka and the filesystem use the same stream separator
        case Frameworks.KAFKA:
            nameComponents = chunkName.split(DEFAULT_STREAM_SEPARATOR);
            if (nameComponents.length < 2) {
                return null;
            }
            // If the scope is needed, extract the topic name from <topicName>-<randId>
            return index == 0 ? nameComponents[0].split(KAFKA_TOPIC_SEPARATOR)[0] : nameComponents[index];

        case Frameworks.PULSAR:
            // Since Pulsar does not have a stream/scope identifier, it is expected to have
            // a global Pulsar policy for the time being. The POJO will have stream and
            // scope both set to "pulsar"
            return "pulsar";

        // This case is for filesystem experimentation and files other than Kafka's .log
        // and Pulsar's ledger files
        case Frameworks.DEFAULT:
            nameComponents = chunkName.split(DEFAULT_STREAM_SEPARATOR);
            if (nameComponents.length < 2) {
                return null;
            }
            return nameComponents[index];

        default:
            return null;
        }
    }

    private static Frameworks getSystemFromChunk(String chunkName) {
        Matcher matcher = StreamPartitionPojo.KAFKA_PARTITION_OBJECT_PATTERN.matcher(chunkName);
        if (matcher.matches())
            return Frameworks.KAFKA;

        matcher = StreamPartitionPojo.PULSAR_PARTITION_OBJECT_PATTERN.matcher(chunkName);
        if (matcher.matches())
            return Frameworks.PULSAR;

        matcher = StreamPartitionPojo.DEFAULT_PARTITION_OBJECT_PATTERN.matcher(chunkName);
        if (matcher.matches())
            return Frameworks.DEFAULT;

        return null;
    }
}
