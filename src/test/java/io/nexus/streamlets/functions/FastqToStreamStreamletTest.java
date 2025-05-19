package io.nexus.streamlets.functions;

import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.utils.StreamletIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static org.mockito.Mockito.*;

public class FastqToStreamStreamletTest {
    private FastqToStreamStreamlet streamlet;
    private FastqToStreamStreamlet.StreamProducer mockProducer;
    private StreamletContext mockContext;
    private StreamletIO mockIO;
    private Policy mockPolicy;
    private Logger mockLogger;

    @BeforeEach
    void setup() throws Exception {
        streamlet = new FastqToStreamStreamlet();
        mockProducer = mock(FastqToStreamStreamlet.StreamProducer.class);
        mockContext = mock(StreamletContext.class);
        mockIO = mock(StreamletIO.class);
        mockPolicy = mock(Policy.class);
        mockLogger = mock(Logger.class);

        when(mockContext.getPolicy()).thenReturn(mockPolicy);
        when(mockContext.getLogger()).thenReturn(mockLogger);
        when(mockPolicy.getStreamletArgumentsByName("FASTQ_TO_STREAM")).thenReturn(Collections.emptyList());

        // Inject mock producer and mark initialized
        var producerField = FastqToStreamStreamlet.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(streamlet, mockProducer);

        var initField = FastqToStreamStreamlet.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(streamlet, true);
    }

    @Test
    void testParsingRealFastqFile() throws Exception {
        InputStream fastqInputStream = Files.newInputStream(
                Paths.get(getClass().getClassLoader().getResource("sample.fastq").toURI())
        );

        when(mockIO.input()).thenReturn(fastqInputStream);

        streamlet.processPutBytes(mockIO, mockContext);

        verify(mockProducer, atLeast(1)).send(any());
        verify(mockProducer).flush();
    }
}

