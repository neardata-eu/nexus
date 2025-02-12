package io.nexus.streamlets.utils;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * A record encapsulating the stream-based input for streamlets.
 */

public record StreamletIO(InputStream input, OutputStream output) {
}