package io.nexus.streamlets.functions;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.engine.Engine;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.modality.cv.translator.YoloV5Translator;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.Translator;
import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.Deserializer;
import io.nexus.streamlets.EventStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ImageClassificationStreamlet extends EventStreamlet<byte[]> {
    private final Logger logger = LoggerFactory.getLogger(ImageClassificationStreamlet.class);

    final static String INFERENCE_MODELS_PATH = "/app/models/";
    final static String INFERENCE_KEY = "inference-result";
    final static Set<String> HUMAN_CLASSES = Set.of(
            "person", "man", "woman", "boy", "girl", "bridegroom", "groom", "bride",
            "student", "police_officer", "firefighter", "worker", "athlete"
            // Add more class names from ImageNet that relate to humans
    );
    protected final String name = "IMAGE_CLASSIFICATION";
    protected final String samplingPercentageArgument = "sampling-percentage=";
    protected final Predictor<Image, DetectedObjects> predictor;
    protected final AtomicReference<Double> samplingPercentage = new AtomicReference<>(1.0);
    boolean cudaAvailable;
    
    protected final AtomicReference<String> aiModel;
    protected final AtomicReference<String> synsetFile;
    /**
     * Constructs a RecordStreamlet with the given deserializer and serializer.
     *
     * @param deserializer The deserializer to convert input data into records.
     */
    public ImageClassificationStreamlet(Deserializer<byte[]> deserializer) {
        super(deserializer);

        //Loading the model based on the respective hardware available
        cudaAvailable = Engine.getInstance().getGpuCount() > 0;
        logger.info(cudaAvailable? "CUDA available. Loading GPU model...":"NO CUDA detected. Loading CPU model...");
        aiModel= new AtomicReference<>(cudaAvailable? "yolov5s-gpu.torchscript.pt" : "yolov5.torchscript.pt");
        synsetFile= new AtomicReference<>("coco.names");
    
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
            Classifications detectedObjects = detectObjects(imageBytes);
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

    protected ZooModel<Image, DetectedObjects> loadModel() throws IOException, ModelNotFoundException, MalformedModelException {
        Map<String, Object> arguments = new ConcurrentHashMap<>();
        arguments.put("width", 640);
        arguments.put("height", 640);
        arguments.put("resize", true);
        arguments.put("rescale", true);

        Translator<Image, DetectedObjects> translator = YoloV5Translator.builder(arguments)
                .optSynsetArtifactName(synsetFile.get())
                .build();

        Criteria<Image, DetectedObjects> criteria =
                Criteria.builder()
                        .setTypes(Image.class, DetectedObjects.class)
                        .optModelPath(resolveModelPath(INFERENCE_MODELS_PATH, ClassLoader.getSystemResource("models/").getPath()))
                        .optEngine("PyTorch")
                        .optModelName(aiModel.get())
                        .optTranslator(translator)
                        .optProgress(new ProgressBar())
                        .optDevice(cudaAvailable? Device.gpu() : Device.cpu())
                        .build();

        return criteria.loadModel();
    }

    /**
     * This method loads the model either from the expected location in the Docker image or in the project's local path.
     *
     * @param preferredPath Preferred AI model path.
     * @param fallbackPath Fallback AI model path.
     * @return Actual path for the AI model to be loaded.
     */
    static Path resolveModelPath(String preferredPath, String fallbackPath) {
        Path preferred = Paths.get(preferredPath);
        if (Files.exists(preferred)) {
            System.out.println("Using preferred model path: " + preferred);
            return preferred;
        } else {
            System.out.println("Preferred model path not found, falling back to: " + fallbackPath);
            return Paths.get(fallbackPath);
        }
    }
}
