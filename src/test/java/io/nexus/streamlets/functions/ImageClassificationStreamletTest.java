package io.nexus.streamlets.functions;

import ai.djl.modality.Classifications;
import ai.djl.translate.TranslateException;
import io.nexus.streamlets.deserializers.KafkaImageDeserializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class ImageClassificationStreamletTest {

    @Test
    void testModelHuman() throws IOException, TranslateException {
        // Load sample image
        ImageClassificationStreamlet streamlet = new KafkaImageClassificationStreamlet(new KafkaImageDeserializer());
        for (int i = 0; i < 100; i++) {
            long startTime = System.currentTimeMillis();
            try (InputStream is = getClass().getResourceAsStream("/images/human.jpg")) {
                assertNotNull(is, "Test image not found in resources!");
                byte[] imageBytes = readAllBytes(is);
                Classifications detectedObjects = streamlet.detectObjects(imageBytes);  // Should return detected objects
                System.out.println("Detected objects: " + detectedObjects);
                assertNotNull(detectedObjects);
                assertFalse(detectedObjects.items().isEmpty());
                boolean containsHuman = detectedObjects.items().stream()
                        .anyMatch(item -> ImageClassificationStreamlet.HUMAN_CLASSES.contains(item.getClassName()));
                assertTrue(containsHuman, "Expected detected objects to include human-related classes");
            }
            System.err.println("INFERENCE TIME: " + (System.currentTimeMillis() - startTime) + "ms");
        }
    }

     @Test
    void testModelNonHuman() throws IOException, TranslateException {
        // Load sample image
        ImageClassificationStreamlet streamlet = new KafkaImageClassificationStreamlet(new KafkaImageDeserializer());
        for (int i = 0; i < 100; i++) {
            long startTime = System.currentTimeMillis();
            try (InputStream is = getClass().getResourceAsStream("/images/kitten.jpg")) {
                assertNotNull(is, "Test image not found in resources!");
                byte[] imageBytes = readAllBytes(is);
                Classifications detectedObjects = streamlet.detectObjects(imageBytes);  // Should return detected objects
                System.out.println("Detected objects: " + detectedObjects);
                assertNotNull(detectedObjects);
                assertFalse(detectedObjects.items().isEmpty());
                boolean containsHuman = detectedObjects.items().stream()
                        .anyMatch(item -> ImageClassificationStreamlet.HUMAN_CLASSES.contains(item.getClassName()));
                assertFalse(containsHuman, "Expected detected objects to include non-human classes");
            }
            System.err.println("INFERENCE TIME: " + (System.currentTimeMillis() - startTime) + "ms");
        }
    }


    private byte[] readAllBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[8192];
        int nRead;
        while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        return buffer.toByteArray();
    }
}
