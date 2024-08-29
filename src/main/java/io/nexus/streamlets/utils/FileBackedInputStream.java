package io.nexus.streamlets.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public class FileBackedInputStream extends InputStream {

    private final Path filePath;
    private FileInputStream fileInputStream;
    private long currentPosition;
    private volatile boolean isClosed = false;
    private volatile boolean noMoreData = false;

    public FileBackedInputStream(File file) throws IOException {
        this.filePath = file.toPath();
        this.fileInputStream = new FileInputStream(file);
        this.currentPosition = 0;
    }

    @Override
    public int read() throws IOException {
        if (isClosed) {
            throw new IOException("Stream is closed");
        }

        while (true) {
            int byteData = fileInputStream.read();
            if (byteData != -1) {
                currentPosition++;
                return byteData & 0xFF;
            }

            if (noMoreData) {
                return -1;
            }

            // Close and reopen the stream to re-read from the current position
            fileInputStream.close();
            fileInputStream = new FileInputStream(filePath.toFile());
            fileInputStream.skip(currentPosition);

            // Pause briefly to wait for more data
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Thread interrupted while waiting for data", e);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (isClosed) {
            throw new IOException("Stream is closed");
        }

        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int totalBytesRead = 0;

        while (totalBytesRead < len) {
            int bytesRead = fileInputStream.read(b, off + totalBytesRead, len - totalBytesRead);
            if (bytesRead == -1) {
                if (noMoreData) {
                    return totalBytesRead == 0 ? -1 : totalBytesRead;
                }

                // Close and reopen the stream to re-read from the current position
                fileInputStream.close();
                fileInputStream = new FileInputStream(filePath.toFile());
                fileInputStream.skip(currentPosition);

                // Pause briefly to wait for more data
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Thread interrupted while waiting for data", e);
                }
            } else {
                totalBytesRead += bytesRead;
                currentPosition += bytesRead;
            }
        }

        return totalBytesRead;
    }

    /**
     * Signals that no more data will be written to the file.
     */
    public void signalEndOfData() {
        this.noMoreData = true;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            isClosed = true;
            fileInputStream.close();
        }
    }
}

