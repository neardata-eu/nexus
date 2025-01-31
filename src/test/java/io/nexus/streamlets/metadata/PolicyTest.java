package io.nexus.streamlets.metadata;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class PolicyTest {

    private Policy policy;
    private Region region1;
    private Region region2;
    private StreamletExecutionDescriptor sed1;
    private StreamletExecutionDescriptor sed2;
    private StreamletExecutionDescriptor sed3;

    @BeforeEach
    public void setUp() {
        region1 = Region.EDGE;
        region2 = Region.CLOUD;

        StreamletDescriptor streamlet1 = new StreamletDescriptor("Streamlet1", StreamletDescriptor.ExecuteOn.ALL,
                Hardware.NONE, false);
        StreamletDescriptor streamlet2 = new StreamletDescriptor("Streamlet2", StreamletDescriptor.ExecuteOn.ALL,
                Hardware.GPU, true);
        StreamletDescriptor streamlet3 = new StreamletDescriptor("Streamlet3", StreamletDescriptor.ExecuteOn.ALL,
                Hardware.NONE, false);

        sed1 = new StreamletExecutionDescriptor(streamlet1, region1, List.of("arg1"));
        sed2 = new StreamletExecutionDescriptor(streamlet2, region1, List.of("arg2"));
        sed3 = new StreamletExecutionDescriptor(streamlet3, region2, List.of("arg3"));

        policy = new Policy("1", "System1", "Scope1", "Stream1",
                List.of(sed1, sed2, sed3), List.of("Storage1"));
    }

    @Test
    public void testGetStreamletsForRegion() {
        List<StreamletExecutionDescriptor> streamlets = policy.getStreamletsForRegion(region1);
        Assertions.assertEquals(2, streamlets.size());
        Assertions.assertTrue(streamlets.contains(sed1));
        Assertions.assertTrue(streamlets.contains(sed2));
    }

    @Test
    public void testGetNextRegionToForward() {
        Assertions.assertEquals(region2, policy.getNextRegionToForward(region1));
        Assertions.assertNull(policy.getNextRegionToForward(region2));
    }

    @Test
    public void testGetSpecialHardwareInRegion() {
        Optional<Hardware> hardware = policy.getSpecialHardwareInRegion(region1);
        Assertions.assertTrue(hardware.isPresent());
        Assertions.assertEquals(Hardware.GPU, hardware.get());

        hardware = policy.getSpecialHardwareInRegion(region2);
        Assertions.assertFalse(hardware.isPresent());
    }

    @Test
    public void testCanSwarmletExecuteStreamlets() {
        Assertions.assertFalse(policy.canSwarmletExecuteStreamlets(region1, Hardware.NONE));
        Assertions.assertTrue(policy.canSwarmletExecuteStreamlets(region2, Hardware.NONE));
        Assertions.assertTrue(policy.canSwarmletExecuteStreamlets(region2, Hardware.GPU));
    }
}
