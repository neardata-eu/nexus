package io.nexus.streamlets.functions;

import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.DataSourceStreamlet;
import io.nexus.streamlets.StreamPartition;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.S3StorageConfig;
import io.nexus.streamlets.utils.FastqRecord;
import io.nexus.streamlets.utils.SerializationUtils;
import io.nexus.streamlets.utils.StreamletIO;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArraySerializer;

import io.pravega.client.ClientConfig;
import org.slf4j.Logger;

public class FastqToStreamStreamlet extends ByteStreamlet implements DataSourceStreamlet {

    private static final String STREAMLET_NAME = "FASTQ_TO_STREAM";
    private static final String STREAMING_SYSTEM_BACKEND_ARG = "streaming-backend";
    private static final String STREAMING_SYSTEM_ENDPOINT_ARG = "streaming-endpoint";
    private static final String SCOPE_NAME_ARG = "scope-name";
    private static final String STREAM_NAME_ARG = "stream-name";
    private volatile boolean initialized = false;
    private volatile StreamProducer producer;

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        tryInitializeProducer(context);
        // Parse the lines of the FASTQ file and convert them into FastqRecord objects.
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(dataStreams.input()))) {
            String line;
            int lineCount = 0;
            String[] fastqRecord = new String[4];
            int recordId = 0;
            while ((line = reader.readLine()) != null) {
                fastqRecord[lineCount++ % 4] = line;
                if (lineCount % 4 == 0) {
                    if (!isHumanSequence(fastqRecord[1])) {
                        FastqRecord record = new FastqRecord(
                                recordId++,
                                fastqRecord[0],
                                fastqRecord[1],
                                fastqRecord[2],
                                fastqRecord[3]
                        );
                        producer.send(record);
                    }
                }
            }
            producer.flush();
            tryCloseProducer();
            context.getLogger().info("PUT - Streamlet [{}] flushed producer [{}]",
                    STREAMLET_NAME, producer.getClass().getSimpleName());
        } catch (IOException e) {
            throw new RuntimeException("Error processing FASTQ input", e);
        }
    }

    private void tryInitializeProducer(StreamletContext context) {
       synchronized (this) {
            if (!initialized) {
                this.producer = initializeProducer(context);
                initialized = true;
            }
        }
    }

    private void tryCloseProducer() {
        synchronized (this) {
            if (initialized) {
                this.producer.close();
                initialized = false;
            }
        }
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        throw new UnsupportedOperationException("Streamlet " + STREAMLET_NAME +
                " is not supposed to implement GET processing.");
    }

    @Override
    public InputStream handlePreGet(StreamPartition streamPartition, StreamletContext context) {
        throw new UnsupportedOperationException("Streamlet " + STREAMLET_NAME +
                " is not supposed to implement pre-GET processing.");
    }

    private boolean isHumanSequence(String dna) {
        // Fake rule: Real impl would compare against human genome segments
        return dna.contains("ACTGACTG");
    }

    private StreamProducer initializeProducer(StreamletContext context) {
        List<String> args = context.getPolicy().getStreamletArgumentsByName(getClass().getName());
        Map<String, String> config = parseArguments(args);
        String backend = config.getOrDefault(STREAMING_SYSTEM_BACKEND_ARG, "").toLowerCase();
        // Use the object path to build the stream scope/name
        config.put(SCOPE_NAME_ARG, context.getStreamPartition().getScope());
        config.put(STREAM_NAME_ARG, context.getStreamPartition().getStream());

        return switch (backend) {
            case "kafka" -> new KafkaProducerImpl(config);
            case "pulsar" -> new PulsarProducerImpl(config);
            case "pravega" -> new PravegaProducerImpl(config);
            default -> throw new IllegalArgumentException("Unsupported backend: " + backend);
        };
    }

    private Map<String, String> parseArguments(List<String> args) {
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            String[] parts = arg.split("=", 2);
            if (parts.length == 2) {
                map.put(parts[0].trim().toLowerCase(), parts[1].trim());
            }
        }
        return map;
    }

    // === Embedded Producer Interfaces and Implementations ===

    interface StreamProducer {
        void send(FastqRecord record);
        void flush();
        void close();
    }

    private static class KafkaProducerImpl implements StreamProducer {
        private final KafkaProducer<byte[], FastqRecord> kafkaProducer;
        private final String topic;

        KafkaProducerImpl(Map<String, String> config) {
            this.topic = config.get(SCOPE_NAME_ARG) + "-" + config.get(STREAM_NAME_ARG);
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(STREAMING_SYSTEM_ENDPOINT_ARG));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FastqRecord.class.getName());
            this.kafkaProducer = new KafkaProducer<>(props);
        }

        @Override
        public void send(FastqRecord record) {
            kafkaProducer.send(new ProducerRecord<>(topic, record));
        }

        @Override
        public void flush() {
            kafkaProducer.flush();
        }

        @Override
        public void close() {
            this.kafkaProducer.close();
        }
    }

    private static class PulsarProducerImpl implements StreamProducer {
        private final org.apache.pulsar.client.api.Producer<FastqRecord> pulsarProducer;
        private final PulsarClient client;

        PulsarProducerImpl(Map<String, String> config) {
            try {
                this.client = PulsarClient.builder()
                        .serviceUrl(config.get(STREAMING_SYSTEM_ENDPOINT_ARG))
                        .build();
                // Define the schema for FastqRecord
                Schema<FastqRecord> schema = Schema.JSON(FastqRecord.class);
                this.pulsarProducer = client.newProducer(schema)
                        .topic(config.get(SCOPE_NAME_ARG) + "-" + config.get(STREAM_NAME_ARG))
                        .create();
            } catch (PulsarClientException e) {
                throw new RuntimeException("Error initializing Pulsar producer", e);
            }
        }

        @Override
        public void send(FastqRecord record) {
            try {
                pulsarProducer.send(record);
            } catch (PulsarClientException e) {
                throw new RuntimeException("Error sending to Pulsar", e);
            }
        }

        @Override
        public void flush() {
            // No explicit flush needed for Pulsar
        }

        @Override
        public void close() {
            try {
                this.pulsarProducer.close();
                this.client.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class PravegaProducerImpl implements StreamProducer {
        private final EventStreamWriter<FastqRecord> writer;
        private final EventStreamClientFactory clientFactory;

        PravegaProducerImpl(Map<String, String> config) {
            URI controllerURI = URI.create(config.get(STREAMING_SYSTEM_ENDPOINT_ARG));
            String scope = config.get(SCOPE_NAME_ARG);
            String stream = config.get(STREAM_NAME_ARG);
            ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();

            try (StreamManager streamManager = StreamManager.create(clientConfig)) {
                streamManager.createScope(scope);
                streamManager.createStream(scope, stream, StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(1))
                        .build());
                this.clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                this.writer = clientFactory.createEventWriter(
                        stream,
                        new SerializationUtils.JsonSerializer<>(FastqRecord.class),
                        EventWriterConfig.builder().build());
            } catch (Exception e) {
                throw new RuntimeException("Failed to setup Pravega producer", e);
            }
        }

        @Override
        public void send(FastqRecord record) {
            writer.writeEvent(record);
        }

        @Override
        public void flush() {
            writer.flush();
        }

        @Override
        public void close() {
            this.writer.close();
            this.clientFactory.close();
        }
    }

    // Main method just for test purposes
    public static void main(String[] args) throws Exception {
        String filePath = "src/test/resources/sample.fastq";
        String endpoint = "tcp://localhost:9090";
        String scope = "scope-fastq";
        String stream = "stream4";

        FastqToStreamStreamlet streamlet = new FastqToStreamStreamlet();

        try (InputStream inputStream = new java.io.FileInputStream(filePath)) {
            StreamletIO io = new StreamletIO(inputStream, null);
            StreamletContext context = new StreamletContext() {
                @Override
                public Policy getPolicy() {
                    return new Policy() {
                        @Override
                        public List<String> getStreamletArgumentsByName(String name) {
                            return List.of(
                                    "streaming-backend=pravega",
                                    "streaming-endpoint=" + endpoint
                            );
                        }
                    };
                }

                @Override
                public Logger getLogger() {
                    return org.slf4j.LoggerFactory.getLogger("FastqToStreamTest");
                }

                @Override
                public StreamPartition getStreamPartition() {
                    return new StreamPartition("container", scope, stream, "partition");
                }

                @Override
                public List<S3StorageConfig> getS3StorageConfigs() {
                    return List.of(); // Not used for this test
                }

                @Override
                public void routeObjectToPolicyStorage(S3StorageConfig config, InputStream objectContent, long contentLength) {
                    throw new UnsupportedOperationException("Not needed in standalone mode");
                }

                @Override
                public InputStream fetchObjectFromPolicyStorage(S3StorageConfig config, StreamPartition streamPartition) {
                    throw new UnsupportedOperationException("Not needed in standalone mode");
                }

                @Override
                public String getUserMetadata(String key) {
                    return null;
                }

                @Override
                public String putUserMetadata(String key, String value) {
                    return null;
                }
            };

            streamlet.processPutBytes(io, context);
            System.out.println("FASTQ file processed and streamed to Pravega successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error running standalone streamlet: " + e.getMessage());
        }
    }
}
