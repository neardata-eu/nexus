package io.nexus.streamlets;

import io.nexus.configuration.NexusConfig;
import io.nexus.streamlets.functions.NoOpStreamlet;
import io.nexus.streamlets.metadata.Hardware;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.StreamletDescriptor;
import io.nexus.streamlets.metadata.StreamletExecutionDescriptor;
import io.nexus.streamlets.utils.ByteBufferPipelineStream;
import io.nexus.streamlets.utils.StreamNameUtils;
import io.nexus.streamlets.metadata.StreamletDescriptor.ExecuteOn;
import io.pravega.common.util.ByteArraySegment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.MutableContentMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StreamletsExecutorTest {
    private MetadataService metadataService;
    private NexusConfig nexusConfig;
    private StreamletsExecutor streamletsExecutor;

    private Blob mockBlob;
    private MutableBlobMetadata mockBlobMetadata;
    private MutableContentMetadata mockContentMetadata;
    private Payload mockPayload;
    private Policy mockPolicy;

    // Mock Blob constants
    final String mockContainerName = "container";
    final String mockScopeName = "scope";
    final String mockStreamName = "stream";
    final String mockFile = "test.txt";
    final String mockBlobName = mockScopeName + StreamNameUtils.STREAM_SEPARATOR + mockStreamName
            + StreamNameUtils.STREAM_SEPARATOR + mockFile;

    // Mock StreamletDescriptor constants
    final StreamletDescriptor mockPutStreamlet = new StreamletDescriptor("noop-1", ExecuteOn.ALL,
            Hardware.NONE, true);

    // Mock Policy constants
    final String mockPolicySystem = "system";
    final List<StreamletExecutionDescriptor> mockPolicyPipeline = new ArrayList<>(List.of(
            new StreamletExecutionDescriptor(mockPutStreamlet, Region.EDGE, Collections.emptyList())));

    final StreamletDescriptor mockGetStreamlet = new StreamletDescriptor("noop-1", ExecuteOn.GET,
            Hardware.NONE, true);

    @BeforeEach
    void setUp() {
        // Mocking the dependencies' objects
        metadataService = Mockito.mock(MetadataService.class);
        nexusConfig = Mockito.mock(NexusConfig.class);
        streamletsExecutor = new StreamletsExecutor(metadataService);
        streamletsExecutor.getFunctionSupplierMap().put("noop-1", new NoOpStreamlet("NOOP"));

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
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, mockContainerName, mockBlob, true);

        // Verify that it does not proceed with further processing
        verify(mockBlobMetadata, never()).getContentMetadata();
        verify(mockBlob, never()).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testNoPolicyFound() throws Exception {
        // Mock valid scope and stream
        when(mockBlob.getMetadata().getName()).thenReturn(mockBlobName);
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);

        // Mock no policy found
        when(metadataService.getPolicyByStream(mockScopeName, mockStreamName)).thenReturn(null);

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, mockContainerName, mockBlob, true);

        // Verify that it does not proceed with further processing
        verify(mockBlobMetadata, never()).getContentMetadata();
        verify(mockBlob, never()).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testNoStreamletsToExecute() throws Exception {
        // Mock valid scope, stream and policy
        when(mockBlob.getMetadata().getName()).thenReturn(mockBlobName);
        when(metadataService.getPolicyByStream(mockScopeName, mockStreamName)).thenReturn(mockPolicy);
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);

        // Mocking an empty policy pipeline
        when(mockPolicy.getPipeline()).thenReturn(new ArrayList<>());

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, mockContainerName, mockBlob, true);

        // Verify that it does not proceed with further processing
        verify(mockBlobMetadata, never()).getContentMetadata();
        verify(mockBlob, never()).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testPUTInterceptionWithNoPUTStreamlets() throws Exception {
        // Setting up dependencies
        buildMockDataForRequestInterception();

        // Filling the pipeline with GET streamlets
        when(metadataService.getStreamletDescriptor(anyString())).thenReturn(mockGetStreamlet);
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, mockContainerName, mockBlob, true);

        // Verifying that there is no further processing of streamlets
        verify(mockBlobMetadata, never()).getContentMetadata();
        verify(mockBlob, never()).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testGETInterceptionWithNoGETStreamlets() throws Exception {
        // Setting up dependencies
        buildMockDataForRequestInterception();

        // Filling the pipeline with PUT streamlets
        when(metadataService.getStreamletDescriptor(anyString())).thenReturn(mockPutStreamlet);
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);

        // Invoking a GET interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, mockContainerName, mockBlob, false);

        // Verifying that there is no further processing of streamlets
        verify(mockBlobMetadata, never()).getContentMetadata();
        verify(mockBlob, never()).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testPUTBlobProcessing() throws Exception {
        // Setting up dependencies
        buildMockDataForRequestInterception();

        // Filling the pipeline with PUT streamlets
        when(metadataService.getStreamletDescriptor(anyString())).thenReturn(mockPutStreamlet);
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);
        when(mockPolicy.getStreamletsForRegion(Region.EDGE)).thenReturn(mockPolicyPipeline);

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, mockContainerName, mockBlob, true);

        // Verifying that setPayload with a stream argument is called EXACTLY once
        // after processing the blob content
        verify(mockBlob, Mockito.times(1)).setPayload(any(ByteBufferPipelineStream.class));
    }

    @Test
    void testGETBlobProcessing() throws Exception {
        // Setting up dependencies
        buildMockDataForRequestInterception();

        // Filling the pipeline with GET streamlets
        when(metadataService.getStreamletDescriptor(anyString())).thenReturn(mockGetStreamlet);
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);
        when(mockPolicy.getStreamletsForRegion(Region.EDGE)).thenReturn(mockPolicyPipeline);

        // Invoking a GET interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, mockContainerName, mockBlob, false);

        // Verifying that setPayload with a stream argument is called EXACTLY once after processing the blob content
        verify(mockBlob, Mockito.times(1)).setPayload(any(ByteBufferPipelineStream.class));
    }

    // Function to set up mocks for the dependencies needed to intercept
    public void buildMockDataForRequestInterception() throws Exception {
        // Mock valid scope, stream, and policy
        when(mockBlob.getMetadata().getName()).thenReturn(mockBlobName);
        when(metadataService.getPolicyByStream(mockScopeName, mockStreamName)).thenReturn(mockPolicy);

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
        when(mockPolicy.getSystem()).thenReturn(mockPolicySystem);
        when(mockPolicy.getPipeline()).thenReturn(mockPolicyPipeline);
    }
}
