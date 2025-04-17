package io.nexus.streamlets.functions;

import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.translate.TranslateException;
import io.nexus.streamlets.deserializers.ByteArrayDeserializer;
import io.nexus.streamlets.deserializers.KafkaImageDeserializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class ImageClassificationTest {

    @Test
    void testDetectObjects() throws IOException, TranslateException {
        // Load sample image
        ImageClassificationStreamlet streamlet = new ImageClassificationStreamlet(new KafkaImageDeserializer());
        for (int i = 0; i < 100; i++) {
            long startTime = System.currentTimeMillis();
            try (InputStream is = getClass().getResourceAsStream("/images/test_22.JPEG")) {
                assertNotNull(is, "Test image not found in resources!");
                byte[] imageBytes = readAllBytes(is);
                DetectedObjects detectedObjects = streamlet.detectObjects(imageBytes);  // Should return detected objects
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
