package io.nexus.streamlets.functions;

import com.google.common.annotations.VisibleForTesting;
import io.nexus.streamlets.ByteStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.utils.StreamletIO;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipException;

public class FastqGzipIndexingStreamlet extends ByteStreamlet {
    private static final int RECORDS_PER_BLOCK = 100000; // Each FASTQ record is 4 lines
    private static final String GZIP_INDEX_METADATA_TAG = "fastqgzip-index";
    private static final int GZIP_COMPRESSION_LEVEL = 1;

    @Override
    protected void processPutBytes(StreamletIO dataStreams, StreamletContext context) {
        try (
                BufferedInputStream in = new BufferedInputStream(dataStreams.input(), 64 * 1024);
                BufferedOutputStream out = new BufferedOutputStream(dataStreams.output(), 64 * 1024)
        ) {
            List<Long> blockOffsets = fastqGzipIndexing(in, out, context);
            String compactIndex = blockOffsets.stream().map(String::valueOf).collect(Collectors.joining(","));
            context.getLogger().info(compactIndex);
            context.putUserMetadata(GZIP_INDEX_METADATA_TAG, compactIndex);
        } catch (IOException e) {
            throw new RuntimeException("FASTQ GZIP indexing failed", e);
        }
    }

        @VisibleForTesting
        List<Long> fastqGzipIndexing(InputStream in, OutputStream out, StreamletContext context) throws IOException {
            List<Long> blockOffsets = new ArrayList<>();
            long currentOffset = 0;

            BufferedReader reader = new BufferedReader(new InputStreamReader(in), 64 * 1024);
            ReusableByteArrayOutputStream blockBuffer = new ReusableByteArrayOutputStream(256 * 1024);
            String line;
            int lineCount = 0;
            int recordCount = 0;
            boolean foundFirstAt = false;

            while ((line = reader.readLine()) != null) {
                byte[] lineBytes = (line + "\n").getBytes(StandardCharsets.UTF_8);

                if (!foundFirstAt) {
                    if (line.startsWith("@")) {
                        foundFirstAt = true;
                        lineCount = 1;

                        // compress and write data before first @ (excluding this line)
                        if (blockBuffer.size() > 0) {
                            context.getLogger().info("COMPRESSING INITIAL DATA " + currentOffset);
                            byte[] compressed = gzipCompress(blockBuffer.getBuffer(), 0, blockBuffer.size());
                            out.write(compressed);
                            currentOffset += compressed.length;
                        }
                        blockBuffer.reset(); // start new block from first @
                    } else {
                        blockBuffer.write(lineBytes); // only write non-record header lines before first @
                        continue;
                    }
                }

                // At this point, foundFirstAt is true, so always write the line
                blockBuffer.write(lineBytes);

                lineCount++;
                if (lineCount == 4) {
                    lineCount = 0;
                    recordCount++;
                    if (recordCount % RECORDS_PER_BLOCK == 0) {
                        context.getLogger().info("CREATING NEW COMPRESSION BLOCK " + recordCount);
                        long iniTime = System.currentTimeMillis();
                        byte[] compressed = gzipCompress(blockBuffer.getBuffer(), 0, blockBuffer.size());
                        context.getLogger().info("COMPRESSION TIME " + (System.currentTimeMillis() - iniTime));
                        out.write(compressed);
                        blockOffsets.add(currentOffset);
                        currentOffset += compressed.length;
                        blockBuffer.reset();
                    }
                }
            }

            if (blockBuffer.size() > 0) {
                byte[] compressed = gzipCompress(blockBuffer.getBuffer(), 0, blockBuffer.size());
                out.write(compressed);
                if (foundFirstAt) {
                    blockOffsets.add(currentOffset);
                }
                currentOffset += compressed.length;
            }

            out.flush();
            return blockOffsets;
        }

    // Reads a line from InputStream, returns number of bytes read or -1 if EOF
    private int readLine(InputStream in, byte[] buffer) throws IOException {
        int total = 0;
        int b;
        while ((b = in.read()) != -1) {
            buffer[total++] = (byte) b;
            if (b == '\n') break;
            if (total >= buffer.length) throw new IOException("Line too long");
        }
        return total == 0 && b == -1 ? -1 : total;
    }

    static class ReusableByteArrayOutputStream extends ByteArrayOutputStream {
        public ReusableByteArrayOutputStream(int size) {
            super(size);
        }

        public byte[] getBuffer() {
            return this.buf;
        }

        public int size() {
            return this.count;
        }
    }

    private static byte[] gzipCompress(byte[] data, int offset, int length) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(length / 2);
        GzipParameters params = new GzipParameters();
        params.setCompressionLevel(GZIP_COMPRESSION_LEVEL); // 1–9
        try (GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(baos, params)) {
            gzip.write(data, offset, length);
        }
        return baos.toByteArray();
    }

    @Override
    protected void processGetBytes(StreamletIO dataStreams, StreamletContext context) {
        try (
                BufferedInputStream in = new BufferedInputStream(dataStreams.input(), 64 * 1024);
                BufferedOutputStream out = new BufferedOutputStream(dataStreams.output(), 64 * 1024)
        ) {
            while (true) {
                try (GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in)) {
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = gzipIn.read(buffer)) != -1) {
                        out.write(buffer, 0, len);
                    }
                } catch (EOFException | ZipException eof) {
                    break;
                } catch (IOException e) {
                    if (in.read() == -1) break;
                    else throw new IOException("Unexpected stream error", e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("FASTQ decompression failed", e);
        }
    }
}
