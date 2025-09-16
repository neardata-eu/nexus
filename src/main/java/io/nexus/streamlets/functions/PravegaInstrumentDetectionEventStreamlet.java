package io.nexus.streamlets.functions;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.modality.cv.translator.YoloV5Translator;
import ai.djl.translate.Translator;
import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.EventStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.deserializers.PravegaGStreamerVideoDeserializer;
import io.nexus.streamlets.utils.AbstractAIModelInference;
import io.nexus.streamlets.utils.PravegaGStreamerVideoFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Event streamlet for human detection on video frames from Pravega GStreamer connector.
 * Processes VideoFrame objects containing compressed video data and performs AI inference.
 */
public class PravegaInstrumentDetectionEventStreamlet extends EventStreamlet<PravegaGStreamerVideoFrame> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @VisibleForTesting
    final AbstractAIModelInference<Image, DetectedObjects> modelHelper;
    private final AtomicReference<Double> samplingPercentage = new AtomicReference<>(100.0);

    static final Set<String> HUMAN_CLASSES = Set.of(
            "person", "man", "woman", "boy", "girl", "bridegroom", "groom", "bride",
            "student", "police_officer", "firefighter", "worker", "athlete"
    );

    private static final String SAMPLING_ARG_PREFIX = "sampling-percentage=";
    private final String name = "HUMAN_DETECTION_VIDEO";

    // Frame processing parameters
    private final AtomicReference<Integer> frameSkipInterval = new AtomicReference<>(1);
    private int frameCounter = 0;
    private final VideoFrameConverter frameConverter = new JpegFrameConverter();

    public PravegaInstrumentDetectionEventStreamlet(PravegaGStreamerVideoDeserializer deserializer) {
        super(deserializer);

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

        this.modelHelper = new AbstractAIModelInference<>(
                modelName, synset, args, translator,
                Image.class, DetectedObjects.class
        );
    }

    @Override
    protected void processPutRecord(PravegaGStreamerVideoFrame videoFrame, StreamletContext context) {
        // Use the specialized frame converter
        try {
            byte[] imageBytes = frameConverter.convertToImageBytes(videoFrame);
            if (imageBytes != null) {
                // Process with the converted image bytes
                DetectedObjects detectedObjects = modelHelper.detectObjects(imageBytes);

                boolean isHuman = detectedObjects.items().stream()
                        .anyMatch(item -> HUMAN_CLASSES.contains(item.getClassName().toLowerCase()));

                logger.info("{} detected at timestamp {} ns: {} | Human: {}",
                        name, videoFrame.getTimestamp(), detectedObjects, isHuman);

                if (isHuman) {
                    updateHumanDetectionCounter(context);
                    storeDetectionResult(context, videoFrame, detectedObjects);
                }
            }
        } catch (Exception e) {
            logger.error("Frame conversion failed: {}", e.getMessage(), e);
        }
    }

    @Override
    protected void processGetRecord(PravegaGStreamerVideoFrame record, StreamletContext context) {
        throw new UnsupportedOperationException("GET not supported for video frame classifier.");
    }

    /**
     * Converts BufferedImage to byte array in specified format
     */
    private byte[] convertBufferedImageToBytes(BufferedImage image, String format) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, format, baos);
        return baos.toByteArray();
    }

    private void updateHumanDetectionCounter(StreamletContext context) {
        int current = context.getUserMetadata(AbstractAIModelInference.INFERENCE_KEY) == null
                ? 0 : Integer.parseInt(context.getUserMetadata(AbstractAIModelInference.INFERENCE_KEY));
        context.putUserMetadata(AbstractAIModelInference.INFERENCE_KEY, String.valueOf(current + 1));
    }

    private void storeDetectionResult(StreamletContext context, PravegaGStreamerVideoFrame frame, DetectedObjects objects) {
        // Store detection results with timestamp for potential replay/analysis
        String detectionKey = "detection_" + frame.getTimestamp();
        String detectionData = String.format("timestamp=%d,objects=%d,humans=%d",
                frame.getTimestamp(),
                objects.getNumberOfObjects(),
                (int) objects.items().stream().filter(item ->
                        HUMAN_CLASSES.contains(item.getClassName().toLowerCase())).count()
        );
        context.putUserMetadata(detectionKey, detectionData);
    }

    private void initializeParameters(StreamletContext context) {
        // Initialize sampling percentage
        if (samplingPercentage.get() == null) {
            List<String> args = context.getPolicy().getStreamletArgumentsByName(getClass().getName());
            if (args != null && !args.isEmpty()) {
                for (String arg : args) {
                    if (arg.startsWith(SAMPLING_ARG_PREFIX)) {
                        samplingPercentage.set(Double.valueOf(arg.replace(SAMPLING_ARG_PREFIX, "")));
                    } else if (arg.startsWith("frame-skip=")) {
                        frameSkipInterval.set(Integer.valueOf(arg.replace("frame-skip=", "")));
                    }
                }
            } else {
                samplingPercentage.set(1.0);
                frameSkipInterval.set(1);
            }
            logger.info("Sampling percentage set to {}%, frame skip interval: {}",
                    samplingPercentage.get(), frameSkipInterval.get());
        }
    }
}

/**
 * Interface for video frame format conversion.
 * Implement this based on your specific GStreamer pipeline output format.
 */
interface VideoFrameConverter {
    byte[] convertToImageBytes(PravegaGStreamerVideoFrame frame) throws Exception;
}

/**
 * Example converter for JPEG-compressed frames
 */
class JpegFrameConverter implements VideoFrameConverter {
    @Override
    public byte[] convertToImageBytes(PravegaGStreamerVideoFrame frame) throws Exception {
        byte[] frameData = frame.getFrameData();

        // If already JPEG, return as-is
        if (frameData.length >= 3 &&
                frameData[0] == (byte) 0xFF &&
                frameData[1] == (byte) 0xD8 &&
                frameData[2] == (byte) 0xFF) {
            return frameData;
        }

        // Otherwise, decode and re-encode as JPEG
        BufferedImage bufferedImage = ImageIO.read(new ByteArrayInputStream(frameData));
        if (bufferedImage == null) {
            throw new IOException("Unable to decode frame data as image");
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, "JPEG", baos);
        return baos.toByteArray();
    }
}

/**
 * Example converter for raw RGB frames (if you know width/height)
 */
class RgbFrameConverter implements VideoFrameConverter {
    private final int width;
    private final int height;

    public RgbFrameConverter(int width, int height) {
        this.width = width;
        this.height = height;
    }

    @Override
    public byte[] convertToImageBytes(PravegaGStreamerVideoFrame frame) throws Exception {
        byte[] frameData = frame.getFrameData();

        // Validate expected size for RGB24
        int expectedSize = width * height * 3;
        if (frameData.length != expectedSize) {
            throw new IllegalArgumentException(
                    String.format("Expected %d bytes for %dx%d RGB frame, got %d",
                            expectedSize, width, height, frameData.length));
        }

        // Create BufferedImage from RGB data
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
        byte[] imageData = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
        System.arraycopy(frameData, 0, imageData, 0, frameData.length);

        // Convert to JPEG bytes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "JPEG", baos);
        return baos.toByteArray();
    }
}