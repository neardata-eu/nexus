package io.nexus.streamlets.functions;

import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.util.NDImageUtils;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Activation;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.AbstractAIModelInference;
import io.nexus.streamlets.utils.StreamletIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SurgicalToolsDetectionStreamlet extends ByteStreamlet {

    private static final Logger logger = LoggerFactory.getLogger(SurgicalToolsDetectionStreamlet.class);
    private static final String INFERENCE_KEY = "surgical-tool-detection";
    private static final AtomicReference<AbstractAIModelInference<Image, Classifications>> modelHelper = new AtomicReference<>();
    private final String name = "SURGICAL_TOOL_DETECTION_BYTE";

    public SurgicalToolsDetectionStreamlet() {
        if (modelHelper.get() == null) {
            boolean useCuda = AbstractAIModelInference.isCudaAvailable();
            String modelName = "nct_tool_torchscript.pt";
            String synset = AbstractAIModelInference.getCurrentModelPath() + "/nct_tool_labels.txt";

            Map<String, Object> args = Map.of(
                    "width", 384,
                    "height", 216,
                    "resize", true,
                    "rescale", true,
                    "mean", new float[]{0.485f, 0.456f, 0.406f},
                    "std", new float[]{0.229f, 0.224f, 0.225f},
                    "applySigmoid", true,
                    "threshold", 0.5f
            );

            modelHelper.compareAndSet(null, new AbstractAIModelInference<>(
                    modelName, synset, args,
                    new SurgicalToolTranslator(args, synset),
                    Image.class, Classifications.class
            ));
        }
    }

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        try (InputStream in = dataStreams.input();
             OutputStream out = dataStreams.output();
             ByteArrayOutputStream copyBuffer = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                copyBuffer.write(buffer, 0, bytesRead);
            }

            byte[] imageBytes = copyBuffer.toByteArray();
            logger.info("Running inference on surgical tool detection model");

            Classifications results = modelHelper.get().detectObjects(imageBytes);
            List<String> detectedTools = results.items().stream()
                    .filter(item -> item.getProbability() >= 0.5)
                    .map(Classifications.Classification::getClassName)
                    .collect(Collectors.toList());

            logger.info("Detected tools: {}", detectedTools);
            context.putUserMetadata(INFERENCE_KEY, String.join(",", detectedTools));
            context.getLogger().info("PUT - Executed Streamlet: {}, as part of pipeline: {}",
                    name, context.getPolicy().getPipeline());

        } catch (IOException e) {
            throw new RuntimeException("Error processing input stream", e);
        }
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        throw new UnsupportedOperationException("GET not supported for tool detection.");
    }

    public Classifications detectObjects(byte[] imageBytes) {
        return modelHelper.get().detectObjects(imageBytes);
    }

    public static class SurgicalToolTranslator implements Translator<Image, Classifications> {
        private List<String> synset;
        private final float[] mean, std;
        private final boolean applySigmoid;
        private final float threshold;
        private final int width, height;

        public SurgicalToolTranslator(Map<String, Object> args, String synsetFile) {
            this.width = (int) args.get("width");
            this.height = (int) args.get("height");
            this.mean = (float[]) args.get("mean");
            this.std = (float[]) args.get("std");
            this.applySigmoid = (boolean) args.get("applySigmoid");
            this.threshold = (float) args.get("threshold");

            try (InputStream is = new FileInputStream(synsetFile)) {
                this.synset = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                        .lines().collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("Failed to load synset: " + synsetFile, e);
            }
        }

        @Override
        public NDList processInput(TranslatorContext ctx, Image input) {
            NDManager manager = ctx.getNDManager();
            NDArray array = input.toNDArray(manager);  // RGB by default
            array = NDImageUtils.resize(array, width, height);
            array = array.div(255f);
            array = array.transpose(2, 0, 1);  // Convert to CHW before normalization
            array = NDImageUtils.normalize(array, mean, std);  // Safe now
            return new NDList(array.expandDims(0));
        }

        @Override
        public Classifications processOutput(TranslatorContext ctx, NDList list) {
            NDArray logits = list.singletonOrThrow().squeeze();
            if (applySigmoid) {
                logits = Activation.sigmoid(logits);
            }

            List<Double> probList = new ArrayList<>();
            for (int i = 0; i < logits.size(); i++) {
                probList.add((double) logits.getFloat(i));
            }

            return new Classifications(synset, probList);
        }

        @Override
        public Batchifier getBatchifier() {
            return null;
        }
    }
}
