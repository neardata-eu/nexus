package io.nexus.streamlets.utils;

import java.io.IOException;
import java.io.InputStream;

/**
 * A high-performance piped input stream that reads from a FastPipedOutputStream.
 */
public class FastPipedInputStream extends InputStream {
    private FastPipedOutputStream source;

    public FastPipedInputStream(FastPipedOutputStream source) {
        if (source == null) {
            throw new NullPointerException("Source output stream cannot be null");
        }
        this.source = source;
        source.connect(this);
    }

    protected void setSource(FastPipedOutputStream source) {
        this.source = source;
    }

    @Override
    public int read() throws IOException {
        return source.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return source.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}