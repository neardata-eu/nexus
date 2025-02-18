package io.nexus.streamlets;

import io.nexus.configuration.NexusConfig;
import io.nexus.streamlets.context.RequestContext;
import io.nexus.streamlets.functions.NoOpStreamlet;
import io.nexus.streamlets.metadata.Hardware;
import io.nexus.streamlets.metadata.MetadataService;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.StreamletDescriptor;
import io.nexus.streamlets.metadata.StreamletExecutionDescriptor;
import io.nexus.streamlets.metadata.StreamletDescriptor.ExecuteOn;

import io.pravega.common.io.ByteBufferOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StreamletsExecutorTest {
    private MetadataService metadataService;
    private NexusConfig nexusConfig;
    private StreamletsExecutor streamletsExecutor;
    private Policy mockPolicy;
    // Mock Blob constants
    final String mockContainerName = "container";
    final String mockScopeName = "scope";
    final String mockStreamName = "stream";
    // Mock StreamletDescriptor constants
    final StreamletDescriptor mockPutStreamlet = new StreamletDescriptor("noop-1", ExecuteOn.ALL,
            Hardware.NONE, true, false);
    // Mock Policy constants
    final String mockPolicySystem = "system";
    final List<StreamletExecutionDescriptor> mockPolicyPipeline = new ArrayList<>(List.of(
            new StreamletExecutionDescriptor(mockPutStreamlet, Region.EDGE, Collections.emptyList())));
    final StreamletDescriptor mockGetStreamlet = new StreamletDescriptor("noop-1", ExecuteOn.GET,
            Hardware.NONE, true, false);
    private StreamPartitionPojo streamPartitionPojo;
    private InputStream inputStream;
    private OutputStream outputStream;
    private RequestContext mockRequestContext;

    @BeforeEach
    void setUp() {
        // Mocking the dependencies' objects
        metadataService = Mockito.mock(MetadataService.class);
        nexusConfig = Mockito.mock(NexusConfig.class);
        streamletsExecutor = new StreamletsExecutor(metadataService);
        streamletsExecutor.getFunctionSupplierMap().put("noop-1", new NoOpStreamlet("NOOP"));
        mockPolicy = Mockito.mock(Policy.class);
        streamPartitionPojo = Mockito.mock(StreamPartitionPojo.class);
        mockRequestContext = Mockito.mock(RequestContext.class);
    }
    
    @Test
    void testNoStreamletsToExecute() throws Exception {
        // Mock valid scope and stream
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);

        // Mock no policy found
        when(metadataService.getPolicyByStream(mockScopeName, mockStreamName)).thenReturn(null);

        // Invoking a PUT interception with the mocked blob
        Assertions.assertThrows(NoPolicySetException.class, () ->
                streamletsExecutor.processRequest(mockPolicy, streamPartitionPojo, inputStream, true,
                        mockRequestContext, outputStream));
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
        when(mockPolicy.getPipeline()).thenReturn(mockPolicyPipeline);
        Logger logger = mock(Logger.class);
        when(mockRequestContext.getLogger()).thenReturn(logger);
        when(mockRequestContext.getPolicy()).thenReturn(mockPolicy);
        when(streamPartitionPojo.getScopedObjectName()).thenReturn("test/test/stream/test");
        when(streamPartitionPojo.getScopedPartitionUri()).thenReturn("test/test/stream");
        when(streamPartitionPojo.getStream()).thenReturn("stream");
        when(streamPartitionPojo.getScope()).thenReturn("test");
        when(streamPartitionPojo.getContainer()).thenReturn("test");
        when(streamPartitionPojo.getObject()).thenReturn("test");

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, streamPartitionPojo, inputStream, true,
                mockRequestContext, outputStream).join();
    }

    @Test
    void testGETBlobProcessing() throws Exception {
        // Setting up dependencies
        buildMockDataForRequestInterception();

        // Filling the pipeline with PUT streamlets
        when(metadataService.getStreamletDescriptor(anyString())).thenReturn(mockPutStreamlet);
        when(metadataService.getNexusConfig()).thenReturn(nexusConfig);
        when(nexusConfig.getRegion()).thenReturn(Region.EDGE);
        when(mockPolicy.getStreamletsForRegion(Region.EDGE)).thenReturn(mockPolicyPipeline);
        when(mockPolicy.getPipeline()).thenReturn(mockPolicyPipeline);
        Logger logger = mock(Logger.class);
        when(mockRequestContext.getLogger()).thenReturn(logger);
        when(mockRequestContext.getPolicy()).thenReturn(mockPolicy);
        when(streamPartitionPojo.getScopedObjectName()).thenReturn("test/test/stream/test");
        when(streamPartitionPojo.getScopedPartitionUri()).thenReturn("test/test/stream");
        when(streamPartitionPojo.getStream()).thenReturn("stream");
        when(streamPartitionPojo.getScope()).thenReturn("test");
        when(streamPartitionPojo.getContainer()).thenReturn("test");
        when(streamPartitionPojo.getObject()).thenReturn("test");

        // Invoking a PUT interception with the mocked blob
        streamletsExecutor.processRequest(mockPolicy, streamPartitionPojo, inputStream, false,
                mockRequestContext, outputStream).join();
    }

    // Function to set up mocks for the dependencies needed to intercept
    public void buildMockDataForRequestInterception() throws Exception {
        // Mock valid scope, stream, and policy
        when(metadataService.getPolicyByStream(mockScopeName, mockStreamName)).thenReturn(mockPolicy);

        // Mocking a ByteBufferPipelineStream to be the mockedblob's payload
        inputStream = new ByteArrayInputStream(new byte[]{1, 2, 3, 4, 5});
        inputStream.close();
        outputStream = new ByteBufferOutputStream();

        // Mock a policy for pipeline processing
        when(mockPolicy.getSystem()).thenReturn(mockPolicySystem);
        when(mockPolicy.getPipeline()).thenReturn(mockPolicyPipeline);
    }
}