package io.nexus.streamlets.functions;

import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class FastqGzipIndexingStreamlet extends ByteStreamlet {

    private static final int PREFIX_LENGTH = 8;
    private static final String JSON_START = "{\"id";
    private static final int JSON_START_LENGTH = JSON_START.length();
    private static final int RECORDS_PER_BLOCK = 100; // JSON records per index block
    private static final String GZIP_INDEX_METADATA_TAG = "fastqgzip-index";

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        try (InputStream in = dataStreams.input();
             OutputStream out = dataStreams.output()) {
            List<Long> blockOffsets = fastqGzipIndexing(in, out);
            // Output index
            String compactIndex = blockOffsets.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));
            System.err.println(compactIndex);
            context.putUserMetadata(GZIP_INDEX_METADATA_TAG, compactIndex);
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress FASTQ JSON data with fixed prefix", e);
        }
    }

    @VisibleForTesting
    List<Long> fastqGzipIndexing(InputStream in, OutputStream out) throws IOException {
        List<Long> blockOffsets = new ArrayList<>();
        long currentOffset = 0;
        List<byte[]> currentRecords = new ArrayList<>();
        int recordCount = 0;

        // Step 1: Extract data before `{"id"}` and write to first gzip member
        byte[] firstGzipMember = findRecordStartingAtId(in);
        if (firstGzipMember.length > 0) {
            byte[] compressedFirstMember = gzipCompress(firstGzipMember);
            out.write(compressedFirstMember);
            currentOffset += compressedFirstMember.length;
        }

        // Step 2: Resume normal processing from first complete `{"id"}` record
        byte[] prefixBuffer = new byte[8];
        boolean foundFirstValidRecord = false;
        int b;

        while (true) {
            // Read 8-byte prefix
            int read = in.readNBytes(prefixBuffer, 0, 8);
            if (read < 8) break;

            // Read JSON record
            ByteArrayOutputStream recordBuffer = new ByteArrayOutputStream();
            int braceDepth = 0;
            boolean started = false;
            boolean insideQuotes = false;
            boolean escaped = false;

            while ((b = in.read()) != -1) {
                recordBuffer.write(b);

                if (b == '"' && !escaped) insideQuotes = !insideQuotes;
                if (!insideQuotes) {
                    if (b == '{') braceDepth++;
                    else if (b == '}') braceDepth--;
                }

                escaped = (b == '\\') && !escaped;

                if (started && braceDepth == 0) break;
                if (!started && b == '{') started = true;
            }

            if (recordBuffer.size() == 0) break;

            byte[] fullRecord = concatenate(List.of(prefixBuffer, recordBuffer.toByteArray()));

            // Check for first valid record
            if (!foundFirstValidRecord && containsId(fullRecord)) {
                foundFirstValidRecord = true;
            }

            currentRecords.add(fullRecord);
            recordCount++;

            if (recordCount == RECORDS_PER_BLOCK) {
                byte[] block = concatenate(currentRecords);
                byte[] compressed = gzipCompress(block);

                if (foundFirstValidRecord) {
                    blockOffsets.add(currentOffset);
                }

                out.write(compressed);
                currentOffset += compressed.length;

                currentRecords.clear();
                recordCount = 0;
            }
        }

        // Flush remaining records
        if (!currentRecords.isEmpty()) {
            byte[] block = concatenate(currentRecords);
            byte[] compressed = gzipCompress(block);

            if (foundFirstValidRecord) {
                blockOffsets.add(currentOffset);
            }

            out.write(compressed);
            currentOffset += compressed.length;
        }

        return blockOffsets;
    }

    public byte[] findRecordStartingAtId(InputStream in) throws IOException {
        final byte[] target = "{\"id\"".getBytes();
        final int targetLength = target.length;

        byte[] prefixBuffer = new byte[8];  // Stores preceding bytes
        int prefixIndex = 0;

        ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
        int matchIndex = 0;

        int b;
        while ((b = in.read()) != -1) {
            // Store preceding 8 bytes in circular buffer
            prefixBuffer[prefixIndex % 8] = (byte) b;
            prefixIndex++;

            // Match `{"id"` sequence
            if (b == target[matchIndex]) {
                matchIndex++;
                if (matchIndex == targetLength) {
                    // We found the sequence! Store the preceding 8 bytes
                    for (int j = 0; j < 8; j++) {
                        outputBuffer.write(prefixBuffer[(prefixIndex - 8 + j) % 8]);
                    }

                    // Write `{"id"` itself
                    outputBuffer.write(target);

                    // Continue reading JSON entry
                    while ((b = in.read()) != -1) {
                        outputBuffer.write(b);
                        if (b == '}') break; // Assume JSON entry ends at '}'
                    }
                    return outputBuffer.toByteArray();
                }
            } else {
                matchIndex = (b == target[0]) ? 1 : 0; // Reset match tracking
            }
        }

        return new byte[0]; // Return empty if `{"id"}` not found
    }

    // Checks whether the byte array contains a valid start of a JSON record
    private boolean containsId(byte[] recordBytes) {
        for (int i = 0; i < recordBytes.length - 5; i++) {
            if (recordBytes[i] == '{' &&
                    recordBytes[i + 1] == '"' &&
                    recordBytes[i + 2] == 'i' &&
                    recordBytes[i + 3] == 'd' &&
                    recordBytes[i + 4] == '"') {
                return true;
            }
        }
        return false;
    }

    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(data);
        }
        return baos.toByteArray();
    }

    private static byte[] concatenate(List<byte[]> chunks) {
        int totalLength = chunks.stream().mapToInt(arr -> arr.length).sum();
        byte[] result = new byte[totalLength];
        int pos = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, pos, chunk.length);
            pos += chunk.length;
        }
        return result;
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        try (InputStream in = dataStreams.input();
             OutputStream out = dataStreams.output()) {

            // Iterate over concatenated GZIP members
            while (true) {
                // Wrap each GZIP block with a GZIPInputStream
                try (GZIPInputStream gzipIn = new GZIPInputStream(in)) {
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = gzipIn.read(buffer)) != -1) {
                        out.write(buffer, 0, len);
                    }
                } catch (EOFException eof) {
                    // All GZIP members read
                    break;
                } catch (IOException e) {
                    // End of stream or bad GZIP — check if it's a valid break
                    if (in.read() == -1) {
                        break; // Normal end of stream
                    } else {
                        throw new IOException("Unexpected data while decompressing blocks", e);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to decompress GZIP file for Pravega output", e);
        }
    }
}



