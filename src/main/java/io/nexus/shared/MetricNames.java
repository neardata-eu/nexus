package io.nexus.shared;

public class MetricNames {

        public static final String PREFIX = "nexus.";

        // Request Interception
        public static final MetricInfo PUT_REQUEST_INTERCEPTION_DURATION = new MetricInfo(
                        PREFIX + "streamlet.interceptor.PUT_request_interception_duration",
                        "Duration of PUT request interception in the streamlet interceptor");

        public static final MetricInfo GET_REQUEST_INTERCEPTION_DURATION = new MetricInfo(
                        PREFIX + "streamlet.interceptor.GET_request_interception_duration",
                        "Duration of GET request interception in the streamlet interceptor");

        public static final MetricInfo MULTIPART_REQUEST_INTERCEPTION_DURATION = new MetricInfo(
                        PREFIX + "streamlet.interceptor.single_multipart_request_interception_duration",
                        "Duration of a single multipart request interception in the streamlet interceptor");

        public static final MetricInfo MULTIPART_EVENT_INTERCEPTION_DURATION = new MetricInfo(
                        PREFIX + "streamlet.interceptor.multipart_event_interception_duration",
                        "Duration of a multipart upload event in the streamlet interceptor");

        // Streamlet Execution
        public static final MetricInfo STREAMLET_POLICY_METADATA_RETRIEVAL_DURATION = new MetricInfo(
                        PREFIX + "streamlet.executor.policy_metadata_retrieval_duration",
                        "Duration of streamlet policy metadata retrieval execution");

        public static final MetricInfo STREAMLET_PIPELINE_BUILD_DURATION = new MetricInfo(
                        PREFIX + "streamlet.executor.streamlet_pipeline_execution_duration",
                        "Duration of streamlet pipeline building");

        // Streamlet Functions
        // TODO: Introduce distinct metrics for each streamlet rather than one for all
        public static final MetricInfo STREAMLET_FUNCTION_EXECUTION_DURATION = new MetricInfo(
                        PREFIX + "streamlet.function.no_op_duration", "Duration of executing a streamlet");
}