package io.nexus.streamlets.utils;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.engine.Engine;
import ai.djl.inference.Predictor;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract class for all Streamlets that require loading AI models for inference.
 */
public class AbstractAIModelInference<T, R> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAIModelInference.class);
    private static final String MODELS_PATH = "/app/models/";
    public static final String INFERENCE_KEY = "inference-result";

    private final AtomicReference<ZooModel<T, R>> modelRef = new AtomicReference<>();
    private final AtomicReference<Predictor<T, R>> predictorRef = new AtomicReference<>();

    private final String modelName;
    private final String synsetFile;
    private final Map<String, Object> translatorArgs;
    private final Translator<T, R> translator;
    private final Class<T> inputType;
    private final Class<R> outputType;
    private final boolean cudaAvailable;

    public AbstractAIModelInference(String modelName,
                          String synsetFile,
                          Map<String, Object> translatorArgs,
                          Translator<T, R> translator,
                          Class<T> inputType,
                          Class<R> outputType) {
        this.modelName = modelName;
        this.synsetFile = synsetFile;
        this.translatorArgs = translatorArgs;
        this.translator = translator;
        this.inputType = inputType;
        this.outputType = outputType;
        this.cudaAvailable = isCudaAvailable();
    }

    public Predictor<T, R> getPredictor() {
        if (predictorRef.get() == null) {
            synchronized (predictorRef) {
                if (predictorRef.get() == null) {
                    try {
                        ZooModel<T, R> model = loadModel();
                        modelRef.set(model);
                        predictorRef.set(model.newPredictor());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to load model", e);
                    }
                }
            }
        }
        return predictorRef.get();
    }

    private ZooModel<T, R> loadModel() throws IOException, ModelNotFoundException, MalformedModelException {
        Criteria<T, R> criteria = Criteria.builder()
                .setTypes(inputType, outputType)
                .optModelPath(getCurrentModelPath())
                .optEngine("PyTorch")
                .optModelName(modelName)
                .optTranslator(translator)
                .optProgress(new ProgressBar())
                .optDevice(cudaAvailable ? Device.gpu() : Device.cpu())
                .build();
        return criteria.loadModel();
    }

    public static boolean isCudaAvailable() {
        try {
            String forceCpu = System.getProperty("ai.djl.force_cpu", System.getenv("DJL_FORCE_CPU"));
            if ("true".equalsIgnoreCase(forceCpu)) return false;

            String forceNoCuda = System.getProperty("ai.djl.pytorch.gpu", System.getenv("DJL_PYTORCH_GPU"));
            if ("false".equalsIgnoreCase(forceNoCuda)) return false;

            if (!isLibcudaAvailable()) return false;

            return Engine.getInstance().getGpuCount() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isLibcudaAvailable() {
        try {
            ProcessBuilder pb = new ProcessBuilder("ldconfig", "-p");
            Process process = pb.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                return reader.lines().anyMatch(line -> line.contains("libcuda.so"));
            }
        } catch (Exception e) {
            return false;
        }
    }

    public static Path getCurrentModelPath() {
        return resolveModelPath(MODELS_PATH, findProjectResourcesModelsPath());
    }

    private static Path resolveModelPath(String preferredPath, String fallbackPath) {
        Path preferred = Paths.get(preferredPath);
        if (Files.exists(preferred)) {
            return preferred;
        } else {
            return Paths.get(fallbackPath);
        }
    }

    private static String findProjectResourcesModelsPath() {
        try {
            URL classUrl = AbstractAIModelInference.class.getProtectionDomain()
                    .getCodeSource().getLocation();
            Path classPath = Paths.get(classUrl.toURI());
            Path currentPath = classPath;

            while (currentPath != null && currentPath.getParent() != null) {
                if (Files.exists(currentPath.resolve("resources/models"))) {
                    return currentPath.resolve("resources/models").toString();
                } else if (Files.exists(currentPath.resolve("src/main/resources/models"))) {
                    return currentPath.resolve("src/main/resources/models").toString();
                } else if (Files.exists(currentPath.resolve("build/resources/main/models"))) {
                    return currentPath.resolve("build/resources/main/models").toString();
                }
                currentPath = currentPath.getParent();
            }

            return Paths.get(System.getProperty("user.dir"), "resources/models").toString();
        } catch (Exception e) {
            return System.getProperty("user.dir") + "/resources/models";
        }
    }

    @SuppressWarnings("unchecked")
    public R detectObjects(byte[] imageBytes) {
        try {
            T input;
            if (inputType == Image.class) {
                input = (T) ImageFactory.getInstance().fromInputStream(new ByteArrayInputStream(imageBytes));
            } else {
                throw new UnsupportedOperationException("Unsupported input type: " + inputType);
            }

            return getPredictor().predict(input);
        } catch (Exception e) {
            throw new RuntimeException("Detection error", e);
        }
    }
}