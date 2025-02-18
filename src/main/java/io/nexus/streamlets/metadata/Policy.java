package io.nexus.streamlets.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.nexus.streamlets.utils.StreamNameUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class represents a Nexus policy on a data stream. It contains the following information:
 * - Identifier of the policy.
 * - System targeted by the policy (Kafka, Pulsar).
 * - Scope (or tenant, depending on the system) for this policy.
 * - Stream (or topic, depending on the system) for this policy.
 * - Pipeline of Streamlets to be executed as part of this policy. Each {@link StreamletExecutionDescriptor} object
 * in the pipeline captures "where" (Region) a Streamlet should be executed, if it requires specific hardware, and
 * any additional parameters to be passed to the Streamlet during its execution.
 * - List of storage locations to store data to (only for terminal Streamlets).
 *
 * It is important to consider that the Pipeline of a Policy will be interpreted in sequential order: left-to-right
 * for PUTs and right-to-left for GETs. An administrator is responsible for understanding the implications of executing
 * Streamlets in a specific order. Moreover, administrators need to make sure that for the Streamlets performing some
 * kind of transformation of data (e.g., compression), the reverse transformation is properly implemented as well, so
 * the streaming system will retrieve from Nexus exactly the same data it originally stored.
 */
public class Policy {

    // Mock policies are created on-the-fly based on the metadata of objects that have been processed by transformer
    // Streamlets. We create mock policies to execute the reverse transformation on objects with no current policies.
    private final static String MOCK_POLICY = "mock";
    private String id;
    private String system;
    private String scope;
    private String stream;
    private List<StreamletExecutionDescriptor> pipeline;
    private List<String> storage;

    public Policy() {
        // Needed for object (de)serialization.
    }

    public Policy(String id, String system, String scope, String stream, List<StreamletExecutionDescriptor> pipeline, List<String> storage) {
        if (id.equalsIgnoreCase(MOCK_POLICY)) {
            throw new IllegalArgumentException("Reserved policy name, please use another one.");
        }
        this.id = id;
        this.system = system;
        this.scope = scope;
        this.stream = stream;
        this.pipeline = pipeline;
        this.storage = storage;
    }

    public static Policy createMockPolicyForLegacyTransformerStreamlets(List<String> transformerStreamlets, Region region,
                                                                        MetadataService metadataService) {
        Policy policy = new Policy();
        policy.id = MOCK_POLICY;
        policy.system = StreamNameUtils.StreamingSystems.DEFAULT.name();
        policy.scope = MOCK_POLICY;
        policy.stream = MOCK_POLICY;
        policy.pipeline = transformerStreamlets.stream()
                .map(s -> new StreamletExecutionDescriptor(metadataService.getStreamletDescriptor(s), region, Collections.emptyList()))
                .collect(Collectors.toList());
        policy.storage = Collections.emptyList();
        return policy;
    }

    public List<StreamletExecutionDescriptor> getStreamletsForRegion(Region region) {
        return this.pipeline.stream().filter(s -> s.getRegion().equals(region)).toList();
    }

    /**
     * Returns the next Region in the policy based on the input Region provided. If currentRegion does not exist or
     * is the last Region in the pipeline, the method will retun null.
     *
     * @param currentRegion
     * @return Next Region in the policy (or null, if there is none).
     */
    public Region getNextRegionToForward(Region currentRegion) {
        Region nextRegion = null;
        boolean foundCurrentRegion = false;
        for (StreamletExecutionDescriptor sed : this.pipeline) {
            if (foundCurrentRegion && !sed.getRegion().equals(currentRegion)) {
                // Found the next region to forward the request
                return sed.getRegion();
            }
            if (sed.getRegion().equals(currentRegion)) {
                foundCurrentRegion = true;
            }
        }

        // No next region to forward, the pipeline ends in the current region.
        return nextRegion;
    }

    public Optional<Hardware> getSpecialHardwareInRegion(Region region) {
        return getStreamletsForRegion(region)
                .stream()
                .filter(s -> !s.getStreamlet().getHardware().equals(Hardware.NONE))
                .map(s -> s.getStreamlet().getHardware())
                .findFirst();
    }

    public boolean canSwarmletExecuteStreamlets(Region currentRegion, Hardware availableHardware) {
        Optional<Hardware> requiredHardware = getSpecialHardwareInRegion(currentRegion);
        return !getStreamletsForRegion(currentRegion).isEmpty()
                && (requiredHardware.isEmpty() || requiredHardware.get().equals(availableHardware));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        if (id.equalsIgnoreCase(MOCK_POLICY)) {
            throw new IllegalArgumentException("Reserved policy name, please use another one.");
        }
        this.id = id;
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public List<StreamletExecutionDescriptor> getPipeline() {
        return this.pipeline;
    }

    public void setPipeline(List<StreamletExecutionDescriptor> pipeline) {
        this.pipeline = pipeline;
    }

    public List<String> getStorage() {
        return storage;
    }

    public void setStorage(List<String> storage) {
        this.storage = storage;
    }

    @JsonIgnore
    public boolean isMock() {
        return this.id.equals(MOCK_POLICY);
    }

    @Override
    public String toString() {
        return "Policy{" +
                "id='" + id + '\'' +
                ", system='" + system + '\'' +
                ", scope='" + scope + '\'' +
                ", stream='" + stream + '\'' +
                ", pipeline=" + pipeline +
                ", storage=" + storage +
                '}';
    }
}
