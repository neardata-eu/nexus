package io.nexus.streamlets.functions;

import ai.djl.Application;
import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.EventStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.deserializers.KafkaImageDeserializer;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class ImageClassificationStreamlet extends EventStreamlet<byte[]> {

    private final String name = "IMAGE_CLASSIFICATION";
    private final String samplingPercentageArgument = "sampling-percentage=";
    final static String INFERENCE_KEY = "human-images";
    static final Set<String> HUMAN_CLASSES = Set.of(
            "person", "man", "woman", "boy", "girl", "bridegroom", "groom", "bride",
            "student", "police_officer", "firefighter", "worker", "athlete"
            // Add more class names from ImageNet that relate to humans
    );

    private final Predictor<Image, DetectedObjects> predictor;
    private final AtomicReference<Double> samplingPercentage = new AtomicReference<>();

    public ImageClassificationStreamlet(KafkaImageDeserializer deserializer) {
        super(deserializer);
        try {
            ZooModel<Image, DetectedObjects> model = loadModel();
            this.predictor = model.newPredictor();
        } catch (IOException | ModelNotFoundException | MalformedModelException e) {
            throw new RuntimeException("Failed to load model", e);
        }
    }

    @Override
    protected void processPutRecord(byte[] imageBytes, StreamletContext context) {
        initializeSamplingPercentage(context);
        Logger logger = context.getLogger();
        // Perform inference according to the sampling percentage.
        if (new Random().nextDouble() * 100 <= this.samplingPercentage.get()) {
            DetectedObjects detectedObjects = detectObjects(imageBytes);
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
        }
    }

    private void initializeSamplingPercentage(StreamletContext context) {
        if (this.samplingPercentage.get() == null) {
            List<String> arguments = context.getPolicy().getStreamletArgumentsByName(getClass().getName());
            if (arguments == null || arguments.isEmpty()) {
                this.samplingPercentage.set(1.0); // By default, do inference in all images
            } else {
                this.samplingPercentage.set(Double.valueOf(arguments.get(0).replace(samplingPercentageArgument, "")));
            }
            context.getLogger().info("Sampling percentage in streamlet {} set to {}", name, this.samplingPercentage.get());
        }
    }

    @Override
    protected void processGetRecord(byte[] record, StreamletContext context) {
        throw new UnsupportedOperationException("GET not supported for image classifier.");
    }

    @VisibleForTesting
    DetectedObjects detectObjects(byte[] imageBytes) {
        try {
            Image img = ImageFactory.getInstance().fromInputStream(new ByteArrayInputStream(imageBytes));
            return predictor.predict(img);
        } catch (Exception e) {
            throw new RuntimeException("Detection error", e);
        }
    }

    private ZooModel<Image, DetectedObjects> loadModel() throws IOException, ModelNotFoundException, MalformedModelException {
        Criteria<Image, DetectedObjects> criteria = Criteria.builder()
                .optApplication(Application.CV.OBJECT_DETECTION)
                .setTypes(Image.class, DetectedObjects.class)
                // SSD MODEL
                //.optArtifactId("ssd")
                // resnet 50
                .optFilter("backbone", "resnet50") // or "mobilenet"
                .optEngine("PyTorch") // Faster R-CNN models are typically implemented in PyTorch
                .optProgress(new ProgressBar())
                .build();
        return ModelZoo.loadModel(criteria);
    }
}


