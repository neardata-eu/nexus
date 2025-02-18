package io.nexus.streamlets.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.nexus.streamlets.metadata.Policy;
import io.nexus.streamlets.metadata.Region;
import io.nexus.streamlets.metadata.StreamletDescriptor;
import io.nexus.streamlets.metadata.StreamletExecutionDescriptor;
import org.apache.http.Header;
import org.apache.http.HttpResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ObjectTagsUtils {

    private static final String METADATA_TAG_HEADER_PREFIX = "x-amz-meta-";
    private final static String SYSTEM_PREFIX = "nexus-";
    private final static String TRANSFORMER_STREAMLET_PREFIX = "transformerstreamlets-";
    private final static String TRANSFORMER_STREAMLET_PARAMETER_SEPARATOR = ":";
    private final static String TRANSFORMER_STREAMLET_SEPARATOR = ",";

    public static String encodeTransformerStreamletsTag(Region region, Policy policy) {
        return policy.getPipeline().stream()
                .filter(sed -> sed.getRegion().equals(region))
                .map(StreamletExecutionDescriptor::getStreamlet)
                .filter(StreamletDescriptor::isTransformsContent)
                .map(sd -> region + TRANSFORMER_STREAMLET_PARAMETER_SEPARATOR + sd.getId())
                .collect(Collectors.joining(TRANSFORMER_STREAMLET_SEPARATOR));
    }

    public static List<String> decodeTransformerStreamletsTag(String encodedPipeline, Region region) {
        // Example: EDGE:compression-1:,EDGE:compression-2
        List<String> result = new ArrayList<>();
        // Split the input by commas to get individual elements
        String[] elements = encodedPipeline.split(TRANSFORMER_STREAMLET_SEPARATOR);
        for (String element : elements) {
            // Split each element by ":" to separate the prefix from the value
            String[] parts = element.split(TRANSFORMER_STREAMLET_PARAMETER_SEPARATOR, 2);
            // Ensure we have both prefix and value
            if (parts.length == 2 && parts[0].equalsIgnoreCase(region.name())) {
                result.add(parts[1].replace(TRANSFORMER_STREAMLET_PARAMETER_SEPARATOR, ""));
            }
        }
        return result;
    }

    public static String getSystemTransformerStreamletsPrefix() {
        return SYSTEM_PREFIX + TRANSFORMER_STREAMLET_PREFIX;
    }

    public static boolean isSystemKey(String key) {
        return key.startsWith(SYSTEM_PREFIX);
    }

    public static Map<String, String> extractValuesFromHeaders(HttpResponse response, String prefix) {
        Map<String, String> nexusHeaders = new HashMap<>();
        for (Header header : response.getAllHeaders()) {
            String headerName = header.getName();
            if (headerName.toLowerCase().startsWith(prefix)) {
                nexusHeaders.put(headerName.substring(prefix.length()), header.getValue());
            }
        }
        return nexusHeaders;
    }

    public static Map<String, String> extractUserMetadataFromHeaders(HttpResponse response) {
        return extractValuesFromHeaders(response, METADATA_TAG_HEADER_PREFIX);
    }

    public static List<String> getTransformerStreamletsFromRequest(Multimap<String, String> allHeaders, Region currentRegion) {
        List<String> transformerStreamletPipeline = new ArrayList<>();
        for (String key : allHeaders.keySet()) {
            if (key.startsWith(METADATA_TAG_HEADER_PREFIX + getSystemTransformerStreamletsPrefix())) {
                transformerStreamletPipeline.addAll(allHeaders.get(key));
            }
        }
        return (transformerStreamletPipeline.isEmpty()) ? Collections.emptyList() :
                decodeTransformerStreamletsTag(transformerStreamletPipeline.getFirst(), currentRegion);
    }
}
