package io.nexus.streamlets.utils;

import com.amazonaws.util.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDataProducer {

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final int totalMBs;
    private final byte[] imageBytes;

    public TestDataProducer(String bootstrapServers, String topic, int totalMBs, String imagePath) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put("remote.storage.enable", "true");

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.totalMBs = totalMBs;

        try (InputStream is = new FileInputStream(imagePath)) {
            this.imageBytes = IOUtils.toByteArray(is);
        }
    }

    public void sendImageRepeatedly() throws ExecutionException, InterruptedException {
        int imageSize = imageBytes.length;
        int totalBytes = totalMBs * 1024 * 1024;
        int numMessages = totalBytes / imageSize;

        for (int i = 0; i < numMessages; i++) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, imageBytes);
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Sent message to topic: " + metadata.topic() +
                    " partition: " + metadata.partition() + " offset: " + metadata.offset() + " size: " + imageSize);
        }
        producer.close();
    }

    public void verifyImageData(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        AtomicInteger total = new AtomicInteger();
        AtomicInteger matched = new AtomicInteger();

        System.out.println("Verifying consumed messages...");
        int messagesRead = 0;
        while (messagesRead < 10) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10000));
            if (records.isEmpty()) continue;

            for (ConsumerRecord<byte[], byte[]> record : records) {
                total.incrementAndGet();
                if (Arrays.equals(record.value(), imageBytes)) {
                    matched.incrementAndGet();
                }
            }
            messagesRead += 1;
        }
        consumer.close();
        System.out.printf("Verification complete: %d/%d messages matched (%.2f%%)\n",
                matched.get(), total.get(), 100.0 * matched.get() / total.get());
    }

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "topic-images";
        int totalMBs = 3;
        String imagePath = "/home/raul/Documents/workspace/nexus-tiered-stream-manager/" +
                "nexus-tiered-stream-manager/src/test/resources/images/test_22.JPEG"; // kitten.jpg";

        try {
            TestDataProducer producer = new TestDataProducer(bootstrapServers, topic, totalMBs, imagePath);
            producer.sendImageRepeatedly();
            Thread.sleep(60000); // wait for propagation to storage tier if enabled
            producer.verifyImageData(bootstrapServers, "image-verification-group");
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
