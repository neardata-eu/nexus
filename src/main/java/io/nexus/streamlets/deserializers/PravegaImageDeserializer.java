package io.nexus.streamlets.deserializers;

import io.nexus.streamlets.DeserializationResult;
import io.nexus.streamlets.Deserializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Deserializer for JPG files transferred by Pravega.
 * Assumes the SOI is 0xFFD8 and the EOI is 0xFFD9
 */

public class PravegaImageDeserializer implements Deserializer<byte[]> {


    private static final byte[] JPEG_SOI = {(byte) 0xFF, (byte) 0xD8}; // Start of image
    private static final byte[] JPEG_EOI = {(byte) 0xFF, (byte) 0xD9}; // End of image

    @Override
    public DeserializationResult<byte[]> deserializeChunk(InputStream input) throws IOException {
        byte[] data = input.readAllBytes();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        List<byte[]> result = new ArrayList<>();

        int position = 0;
        int limit = buffer.limit();

        while (position < limit - 1) {
            // Look for next SOI marker
            int soiIndex = findNextMarker(buffer, position, JPEG_SOI);
            if (soiIndex == -1 || soiIndex >= limit - 1) {
                break; // No more images
            }

            // look for corresponding EOI after SOI now
            int eoiIndex = findNextMarker(buffer, soiIndex + 2, JPEG_EOI);
            if (eoiIndex == -1 || eoiIndex + 1 >= limit) {
                break; // Incomplete JPEG at end
            }

            // Copy out the entire image
            int jpegLength = eoiIndex + 2 - soiIndex;
            byte[] jpegData = new byte[jpegLength];
            buffer.position(soiIndex);
            buffer.get(jpegData, 0, jpegLength);

            result.add(jpegData);

            position = eoiIndex + 2;
        }

        int bytesConsumed = position;
        buffer.position(0);
        return new DeserializationResult<>(result, bytesConsumed);
    }

    // Finds the index of the next occurrence of the given marker
    private int findNextMarker(ByteBuffer buffer, int start, byte[] marker) {
        int limit = buffer.limit();
        for (int i = start; i <= limit - marker.length; i++) {
            boolean match = true; //Assumes match by default
            for (int j = 0; j < marker.length; j++) {
                if (buffer.get(i + j) != marker[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return i;
            }
        }
        return -1;
    }
}