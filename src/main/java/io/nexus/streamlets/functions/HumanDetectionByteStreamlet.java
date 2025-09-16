package io.nexus.streamlets.functions;

import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.modality.cv.translator.YoloV5Translator;
import ai.djl.translate.Translator;
import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.AbstractAIModelInference;
import io.nexus.streamlets.utils.StreamletIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class HumanDetectionByteStreamlet extends ByteStreamlet {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final AtomicReference<AbstractAIModelInference<Image, DetectedObjects>> modelHelper = new AtomicReference<>();

    private static final Set<String> HUMAN_CLASSES = Set.of(
            "person", "man", "woman", "boy", "girl", "bridegroom", "groom", "bride",
            "student", "police_officer", "firefighter", "worker", "athlete"
    );
    private static final String INFERENCE_KEY = "inference-result";
    private final String name = "HUMAN_DETECTION_BYTE";

    public HumanDetectionByteStreamlet() {
        // Initialize model once, atomically
        if (modelHelper.get() == null) {
            boolean useCuda = AbstractAIModelInference.isCudaAvailable();
            String modelName = useCuda ? "yolov5s-gpu.torchscript.pt" : "yolov5.torchscript.pt";
            String synset = "coco.names";

            Map<String, Object> args = Map.of(
                    "width", 640,
                    "height", 640,
                    "resize", true,
                    "rescale", true
            );

            Translator<Image, DetectedObjects> translator = YoloV5Translator.builder(args)
                    .optSynsetArtifactName(synset)
                    .build();

            modelHelper.compareAndSet(null, new AbstractAIModelInference<>(
                    modelName, synset, args, translator,
                    Image.class, DetectedObjects.class
            ));
        }
    }

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        try (InputStream in = dataStreams.input();
             OutputStream out = dataStreams.output();
             ByteArrayOutputStream copyBuffer = new ByteArrayOutputStream()) {

            // Read data from input, write to both output and accumulation buffer
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                copyBuffer.write(buffer, 0, bytesRead);
            }
            logger.info("Completed transferring input data to output");

            // Get all accumulated bytes for inference
            byte[] imageBytes = copyBuffer.toByteArray();

            logger.info("Attempting to detect objects");
            Classifications detectedObjects = this.modelHelper.get().detectObjects(imageBytes);
            boolean isHuman = detectedObjects.items().stream().anyMatch(item ->
                    HUMAN_CLASSES.contains(item.getClassName()));
            // route or annotate the image based on this decision
            logger.info("Detected objects: " + detectedObjects + " | Human: " + isHuman);
            if (isHuman) {
                int currentHumanCounter = context.getUserMetadata(INFERENCE_KEY) == null ?
                        0 : Integer.parseInt(context.getUserMetadata(INFERENCE_KEY));
                // Increment counter of humans in this chunk
                context.putUserMetadata(INFERENCE_KEY, String.valueOf(currentHumanCounter + 1));
            }

            context.getLogger().info("PUT - Executed Streamlet: " + name + ", as part of pipeline: {}",
                    context.getPolicy().getPipeline());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        throw new UnsupportedOperationException("GET not supported for image classifier.");
    }

    public Classifications detectObjects(byte[] imageBytes) {
        return modelHelper.get().detectObjects(imageBytes);
    }
}
 