package io.nexus.streamlets;

import io.nexus.streamlets.cluster.ClusterRing;
import io.nexus.streamlets.metadata.*;
import io.nexus.streamlets.state.StreamletStateManager;
import org.jclouds.blobstore.BlobStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StreamletsRoutingTest {

    private MetadataService metadataService;
    private ClusterRing clusterRing;
    private StreamletsInterceptor interceptor;

    private static final String HOST_ID = "host-1";

    @BeforeEach
    void setUp() {
        metadataService = mock(MetadataService.class);
        clusterRing = mock(ClusterRing.class);
        BlobStore blobStore = mock(BlobStore.class);
        StreamletStateManager stateManager = mock(StreamletStateManager.class);

        when(clusterRing.getThisNodeHostId()).thenReturn(HOST_ID);

        interceptor = new StreamletsInterceptor(blobStore, metadataService, stateManager, clusterRing);
    }

    // -----------------------------
    // Utility builders and helpers
    // -----------------------------

    private Policy buildPolicy(String id, String system, String scope, String stream, List<StreamletExecutionDescriptor> pipeline) {
        return new Policy(id, system, scope, stream, pipeline, List.of("s3default"));
    }

    private StreamletExecutionDescriptor streamletInRegion(String id, Region region, Hardware hardware, boolean isPartitioned) {
        StreamletDescriptor descriptor = new StreamletDescriptor(
                id,
                StreamletDescriptor.ExecuteOn.ALL,
                hardware,
                isPartitioned,
                false,
                false
        );
        return new StreamletExecutionDescriptor(descriptor, region, List.of());
    }

    private void registerSwarmlet(String endpoint, Region region, Hardware hardware) {
        SwarmletDescriptor swarmlet = new SwarmletDescriptor();
        swarmlet.setRegion(region);
        swarmlet.setHardware(hardware);
        swarmlet.setServiceEndpoint(endpoint);
        when(metadataService.getSwarmletDescriptorByRegionAndHardware(region, hardware)).thenReturn(endpoint);
    }

    private StreamPartition streamPartition(String key) {
        return new StreamPartition("container", "scope", "stream", key);
    }

    // -----------------------------
    // Placeholder for test cases
    // -----------------------------

    @Test
    void testRoutingEdgeToCloudStoragePipeline() {
        // -- Setup: Create a streamlet to run in CLOUD
        StreamletExecutionDescriptor cloudStreamlet = streamletInRegion("cloud-op", Region.CLOUD, Hardware.NONE, false);

        // -- Build the policy pipeline: only streamlets in CLOUD
        Policy policy = buildPolicy("cloud-policy", "kafka", "scopeA", "streamA", List.of(cloudStreamlet));

        // -- Register metadata
        registerSwarmlet("http://edge-swarmlet/", Region.EDGE, Hardware.NONE);
        registerSwarmlet("http://cloud-swarmlet/", Region.CLOUD, Hardware.NONE);

        // -- Simulate cluster ownership for the partition
        StreamPartition partition = streamPartition("partition-123");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID);

        // -- EDGE context: expect routing to CLOUD
        String edgeResult = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition);
        Assertions.assertEquals("http://cloud-swarmlet/", edgeResult, "Expected routing to CLOUD swarmlet");

        // -- CLOUD context: expect no further routing (terminal region)
        String cloudResult = interceptor.getNextRoutingEndpoint(policy, Region.CLOUD, Hardware.NONE, partition);
        Assertions.assertNull(cloudResult, "Expected no further routing in CLOUD (terminal storage region)");
    }

    @Test
    void testRoutingEdgeToCloudGPUStoragePipeline() {
        // -- Setup: Create a streamlet to run in CLOUD
        StreamletExecutionDescriptor cloudStreamlet = streamletInRegion("cloud-op", Region.CLOUD, Hardware.NONE, false);
        StreamletExecutionDescriptor cloudStreamlet2 = streamletInRegion("cloud-op2", Region.CLOUD, Hardware.GPU, false);

        // -- Build the policy pipeline: only streamlets in CLOUD
        Policy policy = buildPolicy("cloud-policy", "kafka", "scopeA", "streamA", List.of(cloudStreamlet, cloudStreamlet2));

        // -- Register metadata
        registerSwarmlet("http://edge-swarmlet/", Region.EDGE, Hardware.NONE);
        registerSwarmlet("http://cloud-swarmlet/", Region.CLOUD, Hardware.GPU);

        // -- Simulate cluster ownership for the partition
        StreamPartition partition = streamPartition("partition-123");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID);

        // -- EDGE context: expect routing to CLOUD
        String edgeResult = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition);
        Assertions.assertEquals("http://cloud-swarmlet/", edgeResult, "Expected routing to CLOUD swarmlet");

        // -- CLOUD context: expect no further routing (terminal region)
        String cloudResult = interceptor.getNextRoutingEndpoint(policy, Region.CLOUD, Hardware.GPU, partition);
        Assertions.assertNull(cloudResult, "Expected no further routing in CLOUD (terminal storage region)");
    }

    @Test
    void testRoutingEdgeOnlyExecutionNoRoutingNeededIfCorrectWorker() {
        StreamletExecutionDescriptor edgeStreamlet = streamletInRegion("edge-op", Region.EDGE, Hardware.NONE, true);
        Policy policy = buildPolicy("edge-only", "kafka", "scope1", "stream1", List.of(edgeStreamlet));

        registerSwarmlet("http://edge-swarmlet/", Region.EDGE, Hardware.NONE);
        registerSwarmlet("http://cloud-swarmlet/", Region.CLOUD, Hardware.NONE);

        StreamPartition partition = streamPartition("edge-partition");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID); // we own the partition

        String edgeResult = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition);
        Assertions.assertNull(edgeResult, "Expected no routing from EDGE when worker owns the partition");

        String cloudResult = interceptor.getNextRoutingEndpoint(policy, Region.CLOUD, Hardware.NONE, partition);
        Assertions.assertNull(cloudResult, "Expected no further routing in CLOUD (terminal storage region)");
    }

    @Test
    void testRoutingEdgeGpuRoutingFromEdgeNoneSwarmlet() {
        // Setup policy with EDGE GPU streamlet and CLOUD NONE streamlet
        StreamletExecutionDescriptor edgeGpuStreamlet = streamletInRegion("edge-gpu-op", Region.EDGE, Hardware.GPU, false);
        StreamletExecutionDescriptor cloudStreamlet = streamletInRegion("cloud-op", Region.CLOUD, Hardware.NONE, false);
        Policy policy = buildPolicy("edge-gpu-cloud", "kafka", "scopeX", "streamX", List.of(edgeGpuStreamlet, cloudStreamlet));

        // Register all swarmlets
        registerSwarmlet("http://edge-none-swarmlet/", Region.EDGE, Hardware.NONE);
        registerSwarmlet("http://edge-gpu-swarmlet/", Region.EDGE, Hardware.GPU);
        registerSwarmlet("http://cloud-swarmlet/", Region.CLOUD, Hardware.NONE);

        StreamPartition partition = streamPartition("partition-edgegpu");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID); // assume we own the partition

        // If on EDGE with NONE hardware, should route to EDGE GPU
        String routeFromEdgeNone = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition);
        Assertions.assertEquals("http://edge-gpu-swarmlet/", routeFromEdgeNone, "Expected intra-region routing to EDGE GPU");

        // If on EDGE GPU and we own the partition → CLOUD
        String routeFromEdgeGpu = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.GPU, partition);
        Assertions.assertEquals("http://cloud-swarmlet/", routeFromEdgeGpu, "Expected routing to CLOUD");

        // If on CLOUD → should not route further
        String routeFromCloud = interceptor.getNextRoutingEndpoint(policy, Region.CLOUD, Hardware.NONE, partition);
        Assertions.assertNull(routeFromCloud, "Expected no routing from CLOUD, terminal region");
    }

    @Test
    void testPartitionOwnershipMismatchTriggersReroute() {
        StreamletExecutionDescriptor edgeStreamlet = streamletInRegion("edge-op", Region.EDGE, Hardware.NONE, true);
        Policy policy = buildPolicy("edge-reroute", "kafka", "scope1", "stream1", List.of(edgeStreamlet));

        StreamPartition partition = streamPartition("some-partition");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn("host-2"); // not us

        // Should reroute to the actual node owning the partition
        String result = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition);
        Assertions.assertEquals("host-2/", result, "Should reroute to partition owner");
    }

    @Test
    void testNoSuitableSwarmletInRegionThrows() {
        StreamletExecutionDescriptor edgeGpuStreamlet = streamletInRegion("gpu-op", Region.EDGE, Hardware.GPU, false);
        Policy policy = buildPolicy("no-suitable", "kafka", "scope1", "stream1", List.of(edgeGpuStreamlet));

        StreamPartition partition = streamPartition("gpu-partition");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID); // we own it

        // No swarmlet registered for EDGE + GPU
        Assertions.assertThrows(NoSuitableSwarmletInRegionException.class, () ->
                interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition)
        );
    }

    @Test
    void testTerminalRegionWithNoStreamletsReturnsNull() {
        // No streamlets in CLOUD
        Policy policy = buildPolicy("terminal-policy", "kafka", "scopeT", "streamT", List.of());

        StreamPartition partition = streamPartition("terminal-partition");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID);

        String result = interceptor.getNextRoutingEndpoint(policy, Region.CLOUD, Hardware.NONE, partition);
        Assertions.assertNull(result, "Terminal region with no streamlets should return null");
    }

    @Test
    void testNoStreamletsInRegionGoesToNextRegion() {
        // Only streamlet in CLOUD
        StreamletExecutionDescriptor cloudStreamlet = streamletInRegion("cloud-op", Region.CLOUD, Hardware.NONE, false);
        Policy policy = buildPolicy("next-region", "kafka", "scopeN", "streamN", List.of(cloudStreamlet));

        registerSwarmlet("http://cloud-swarmlet/", Region.CLOUD, Hardware.NONE);

        StreamPartition partition = streamPartition("skip-partition");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID);

        String result = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition);
        Assertions.assertEquals("http://cloud-swarmlet/", result, "Should route to next region");
    }

    @Test
    void testRealRoutingScenarioPolicyInCloudOnly() {
        // Create the two streamlets for the policy
        StreamletExecutionDescriptor classification = streamletInRegion(
                "io.nexus.streamlets.functions.ByteImageClassificationStreamlet",
                Region.CLOUD, Hardware.GPU, false);

        StreamletExecutionDescriptor routing = streamletInRegion(
                "io.nexus.streamlets.functions.ImageRoutingStreamlet",
                Region.CLOUD, Hardware.NONE, true); // partitionLocality = true

        Policy policy = buildPolicy("img-policy", "kafka", "image-scope", "img-stream", List.of(classification, routing));

        // Register CLOUD GPU swarmlet
        registerSwarmlet("http://nexus2-svc:9092/", Region.CLOUD, Hardware.GPU);
        // Register EDGE NONE swarmlet (should never be routed to)
        registerSwarmlet("http://nexus-svc:9092/", Region.EDGE, Hardware.NONE);

        // Partition owned by this node
        StreamPartition partition = streamPartition("partition-image");
        when(clusterRing.getNodeForKey(partition.getScopedPartitionUri())).thenReturn(HOST_ID);

        // 1. Request lands on EDGE → route to CLOUD GPU
        String resultFromEdge = interceptor.getNextRoutingEndpoint(policy, Region.EDGE, Hardware.NONE, partition);
        Assertions.assertEquals("http://nexus2-svc:9092/", resultFromEdge, "Request from EDGE should go to CLOUD");

        // 2. Request lands on CLOUD GPU → we own the partition → no routing
        String resultFromCloud = interceptor.getNextRoutingEndpoint(policy, Region.CLOUD, Hardware.GPU, partition);
        Assertions.assertNull(resultFromCloud, "Request from CLOUD GPU should not be routed further");
    }
}