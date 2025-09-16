package io.nexus.streamlets.functions;

import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.modality.cv.translator.YoloV5Translator;
import ai.djl.translate.Translator;
import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.EventStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.deserializers.KafkaImageDeserializer;
import io.nexus.streamlets.utils.AbstractAIModelInference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaHumanDetectionEventStreamlet extends EventStreamlet<byte[]> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final AtomicReference<AbstractAIModelInference<Image, DetectedObjects>> modelHelper = new AtomicReference<>();
    private final AtomicReference<Double> samplingPercentage = new AtomicReference<>();
    static final Set<String> HUMAN_CLASSES = Set.of(
            "person", "man", "woman", "boy", "girl", "bridegroom", "groom", "bride",
            "student", "police_officer", "firefighter", "worker", "athlete"
    );

    private static final String SAMPLING_ARG_PREFIX = "sampling-percentage=";
    private final String name = "HUMAN_DETECTION_BYTE";

    public KafkaHumanDetectionEventStreamlet(KafkaImageDeserializer deserializer) {
        super(deserializer);

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
    protected void processPutRecord(byte[] imageBytes, StreamletContext context) {
        initializeSamplingPercentage(context);
        if (new Random().nextDouble() * 100 <= samplingPercentage.get()) {
            DetectedObjects detectedObjects = modelHelper.get().detectObjects(imageBytes);
            boolean isHuman = detectedObjects.items().stream()
                    .anyMatch(item -> HUMAN_CLASSES.contains(item.getClassName()));
            logger.info(name + " detected: " + detectedObjects + " | Human: " + isHuman);
            if (isHuman) {
                int current = context.getUserMetadata(AbstractAIModelInference.INFERENCE_KEY) == null
                        ? 0 : Integer.parseInt(context.getUserMetadata(AbstractAIModelInference.INFERENCE_KEY));
                context.putUserMetadata(AbstractAIModelInference.INFERENCE_KEY, String.valueOf(current + 1));
            }
        }
    }

    @Override
    protected void processGetRecord(byte[] record, StreamletContext context) {
        throw new UnsupportedOperationException("GET not supported for image classifier.");
    }

    private void initializeSamplingPercentage(StreamletContext context) {
        if (samplingPercentage.get() == null) {
            List<String> args = context.getPolicy().getStreamletArgumentsByName(getClass().getName());
            if (args != null && !args.isEmpty()) {
                samplingPercentage.set(Double.valueOf(args.get(0).replace(SAMPLING_ARG_PREFIX, "")));
            } else {
                samplingPercentage.set(100.0);
            }
            logger.info("Sampling percentage set to {}", samplingPercentage.get());
        }
    }

    public Classifications detectObjects(byte[] imageBytes) {
        return modelHelper.get().detectObjects(imageBytes);
    }
}

