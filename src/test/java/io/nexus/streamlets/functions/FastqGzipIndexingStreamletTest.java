package io.nexus.streamlets.functions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertFalse;

class FastqGzipIndexingStreamletTest {

    @Test
    void testFastqGzipIndexingStreamlet() throws IOException {
        File inputFile = new File("src/test/resources/pravega-lts-dna-chunk");
        File outputFile = File.createTempFile("compressed-output", ".gz");

        FastqGzipIndexingStreamlet streamlet = new FastqGzipIndexingStreamlet();

        // Call processPutBytes to compress input
        List<Long> blockOffsets;
        try (FileInputStream fis = new FileInputStream(inputFile);
             FileOutputStream fos = new FileOutputStream(outputFile)) {
            blockOffsets = streamlet.fastqGzipIndexing(fis, fos);
        }

        // Read back index
        assertFalse(blockOffsets.isEmpty(), "Index should contain at least one block");

        // ===== Read back and test blockOffsets =====
        try (RandomAccessFile raf = new RandomAccessFile(outputFile, "r")) {
            for (int i = 0; i < blockOffsets.size(); i++) {
                long offset = blockOffsets.get(i);
                System.err.println("OFFSET " + offset);
                long nextOffset = (i + 1 < blockOffsets.size()) ? blockOffsets.get(i + 1) : raf.length();
                int length = (int) (nextOffset - offset);

                // Skip the fixed 8-byte prefix that precedes each record
                raf.seek(offset);
                byte[] gzippedBlock = new byte[length];
                raf.readFully(gzippedBlock);

                // Decompress GZIP block
                try (GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(gzippedBlock))) {
                    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = gzipIn.read(buffer)) != -1) {
                        decompressed.write(buffer, 0, bytesRead);
                    }
                } catch (IOException ex) {
                    Assertions.fail("Problem decompressing FastQGzip file");
                }
            }
        }
        // Clean up temp file
        //Files.deleteIfExists(outputFile.toPath());
    }
}