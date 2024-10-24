package io.nexus.streamlets;

import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.nexus.streamlets.utils.StreamNameUtils;
import io.nexus.streamlets.metadata.Policy;
import io.pravega.common.util.ByteArraySegment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.MutableContentMetadata;

import java.util.ArrayList;
import java.util.List;

public class StreamletsExecutorTest {
    private MetadataService metadataService;
    private StreamletsExecutor streamletsExecutor;

    private Blob mockBlob;
    private MutableBlobMetadata mockBlobMetadata;
    private MutableContentMetadata mockContentMetadata;
    private Payload mockPayload;
    private Policy mockPolicy;

    // Mock Blob constants
    final String MOCK_CONTAINER_NAME = "container";
    final String MOCK_SCOPE_NAME = "scope";
    final String MOCK_STREAM_NAME = "stream";
    final String MOCK_FILE = "test.txt";
    final String MOCK_BLOB_NAME = MOCK_SCOPE_NAME + StreamNameUtils.STREAM_SEPARATOR + MOCK_STREAM_NAME
            + StreamNameUtils.STREAM_SEPARATOR + MOCK_FILE;

    // Mock Policy constants
    final String MOCK_POLICY_SYSTEM = "system";
    final List<String> MOCK_POLICY_PIPELINE = new ArrayList<>(List.of("noop-1", "noop-2", "noop-3"));

    @BeforeEach
    void setUp() {
        // Mocking the dependencies' objects
        metadataService = Mockito.mock(MetadataService.class);
        streamletsExecutor = new StreamletsExecutor(metadataService);

        mockBlob = Mockito.mock(Blob.class);
        mockBlobMetadata = Mockito.mock(MutableBlobMetadata.class);
        mockContentMetadata = Mockito.mock(MutableContentMetadata.class);
        mockPayload = Mockito.mock(Payload.class);
        mockPolicy = Mockito.mock(Policy.class);

        // Set up the mocked blob's metadata to return the mocked objects
        when(mockBlob.getMetadata()).thenReturn(mockBlobMetadata);
        when(mockBlobMetadata.getContentMetadata()).thenReturn(mockContentMetadata);
    }

    @Test
    void testScopeOrStreamIsNull() {
        // Mock blob name for StreamNameUtils to return null for scope/stream
        when(mockBlob.getMetadata().getName()).thenReturn(null);

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.interceptAndProcessRequest(MOCK_CONTAINER_NAME, mockBlob, true);

        // Verify that it does not proceed with further processing
        verify(mockBlobMetadata, never()).getContentMetadata();
        verify(mockBlob, never()).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testNoPolicyFound() throws Exception {
        // Mock valid scope and stream
        when(mockBlob.getMetadata().getName()).thenReturn(MOCK_BLOB_NAME);

        // Mock no policy found
        when(metadataService.getPolicyByStream(MOCK_SCOPE_NAME, MOCK_STREAM_NAME)).thenReturn(null);

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.interceptAndProcessRequest(MOCK_CONTAINER_NAME, mockBlob, true);

        // Verify that it does not proceed with further processing
        verify(mockBlobMetadata, never()).getContentMetadata();
        verify(mockBlob, never()).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testPUTBlobProcessing() throws Exception {
        // Setting up dependencies
        buildMockDataForRequestInterception();

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.interceptAndProcessRequest(MOCK_CONTAINER_NAME, mockBlob, true);

        // Verifying that setPayload with a stream argument is called exactly once
        // after processing the blob content
        verify(mockBlob, Mockito.times(1)).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testGETBlobProcessing() throws Exception {
        // Setting up dependencies
        buildMockDataForRequestInterception();

        // Invoking a GET interception with the mocked blob
        streamletsExecutor.interceptAndProcessRequest(MOCK_CONTAINER_NAME, mockBlob, false);

        // Verifying that setPayload with a stream argument is called exactly once
        // after processing the blob content
        verify(mockBlob, Mockito.times(1)).setPayload(any(ByteBufferPipelineStream.class));
    }

    // Function to set up mocks for the dependencies needed to process the content
    public void buildMockDataForRequestInterception() throws Exception {
        // Mock valid scope, stream, and policy
        when(mockBlob.getMetadata().getName()).thenReturn(MOCK_BLOB_NAME);
        when(metadataService.getPolicyByStream(MOCK_SCOPE_NAME, MOCK_STREAM_NAME)).thenReturn(mockPolicy);

        // Mocking a ByteBufferPipelineStream to be the mockedblob's payload
        byte[] testData = new byte[] { 0, 1, 2, 3, 4, 5, 6 };
        ByteBufferPipelineStream mockInputStream = new ByteBufferPipelineStream();
        mockInputStream.addSegment(new ByteArraySegment(testData));
        mockInputStream.close();

        // Mock a valid content length and payload
        when(mockContentMetadata.getContentLength()).thenReturn(Long.valueOf(testData.length));

        // Mock a valid payload with the mocked stream
        when(mockBlob.getPayload()).thenReturn(mockPayload);
        when(mockBlob.getPayload().openStream()).thenReturn(mockInputStream);

        // Mock a policy for pipeline processing
        when(mockPolicy.getSystem()).thenReturn(MOCK_POLICY_SYSTEM);
        when(mockPolicy.getPipeline()).thenReturn(MOCK_POLICY_PIPELINE);

    }
}
