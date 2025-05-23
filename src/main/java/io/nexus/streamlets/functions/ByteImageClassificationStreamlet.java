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
import ai.djl.ndarray.NDManager;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.Translator;
import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ByteImageClassificationStreamlet extends ByteStreamlet {
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
    protected final AtomicReference<Predictor<Image, DetectedObjects>> predictor = new AtomicReference<>();
    protected final AtomicReference<Double> samplingPercentage = new AtomicReference<>(1.0);
    boolean cudaAvailable;
    
    protected final AtomicReference<String> aiModel;
    protected final AtomicReference<String> synsetFile;

    public ByteImageClassificationStreamlet() {
        try {
            cudaAvailable = Engine.getInstance().getGpuCount() > 0;
            logger.info(cudaAvailable? "CUDA available. Loading GPU model...":"NO CUDA detected. Loading CPU model...");
            aiModel= new AtomicReference<>(cudaAvailable? "yolov5s-gpu.torchscript.pt" : "yolov5.torchscript.pt");
            synsetFile= new AtomicReference<>("coco.names");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load engine: {}", e);
        }
    }

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {

        Logger logger = context.getLogger();
        if (predictor.get() == null) {
            try {
                logger.info("Loading model");
                ZooModel<Image, DetectedObjects> model = loadModel();
                logger.info("Creating predictor");
                this.predictor.set(model.newPredictor());
            } catch (IOException | ModelNotFoundException | MalformedModelException e) {
                throw new RuntimeException("Failed to load model", e);
            }
        }

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

            context.getLogger().info("PUT - Executed Streamlet: " + name + ", as part of pipeline: {}", context.getPolicy().getPipeline());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        throw new UnsupportedOperationException("GET not supported for image classifier.");
    }

    @VisibleForTesting
    DetectedObjects detectObjects(byte[] imageBytes) {
        try {
            System.out.println("DEBUG: Image bytes length = " + imageBytes.length);
            Image img = ImageFactory.getInstance().fromInputStream(new ByteArrayInputStream(imageBytes));

            NDManager manager = NDManager.newBaseManager();
            System.out.println("DEBUG: Default NDManager device: " + manager.getDevice());

            return predictor.get().predict(img);
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
 