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

        public static final MetricInfo INITIATE_MULTIPART_REQUEST_INTERCEPTION_DURATION = new MetricInfo(
                        PREFIX + "streamlet.interceptor.initiate_multipart_request_interception_duration",
                        "Duration of initiate multipart request interception in the streamlet interceptor");

        public static final MetricInfo ABORT_MULTIPART_REQUEST_INTERCEPTION_DURATION = new MetricInfo(
                PREFIX + "streamlet.interceptor.abort_multipart_request_interception_duration",
                "Duration of abort multipart request interception in the streamlet interceptor");

        public static final MetricInfo MULTIPART_EVENT_INTERCEPTION_DURATION = new MetricInfo(
                        PREFIX + "streamlet.interceptor.multipart_event_interception_duration",
                        "Duration of a multipart upload event in the streamlet interceptor");

        // Request IO
        public static final MetricInfo PUT_REQUEST_STORAGE_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.interceptor.PUT_request_storage_throughput",
                "Throughput of PUT request against S3 storage");

        public static final MetricInfo PUT_REQUEST_STORAGE_SIZE = new MetricInfo(
                PREFIX + "streamlet.interceptor.PUT_request_storage_size",
                "Size of PUT request against S3 storage");

        public static final MetricInfo PUT_REQUEST_STORAGE_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.interceptor.PUT_request_storage_operations",
                "PUT request operations against S3 storage");

        public static final MetricInfo PUT_REQUEST_FORWARD_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.interceptor.PUT_request_forward_throughput",
                "Throughput of PUT request forward to another Nexus instance");

        public static final MetricInfo PUT_REQUEST_FORWARD_SIZE = new MetricInfo(
                PREFIX + "streamlet.interceptor.PUT_request_forward_size",
                "Size of PUT request forward to another Nexus instance");

        public static final MetricInfo PUT_REQUEST_FORWARD_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.interceptor.PUT_request_forward_operations",
                "PUT request operations forwarded to another Nexus instance");

        public static final MetricInfo GET_REQUEST_STORAGE_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.interceptor.GET_request_storage_throughput",
                "Throughput of GET request against S3 storage");

        public static final MetricInfo GET_REQUEST_STORAGE_SIZE = new MetricInfo(
                PREFIX + "streamlet.interceptor.GET_request_storage_size",
                "Size of GET request against S3 storage");

        public static final MetricInfo GET_REQUEST_STORAGE_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.interceptor.GET_request_storage_operations",
                "GET request operations against S3 storage");

        public static final MetricInfo GET_REQUEST_FORWARD_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.interceptor.GET_request_forward_throughput",
                "Throughput of GET request forward to another Nexus instance");

        public static final MetricInfo GET_REQUEST_FORWARD_SIZE = new MetricInfo(
                PREFIX + "streamlet.interceptor.GET_request_forward_size",
                "Size of GET request forward to another Nexus instance");

        public static final MetricInfo GET_REQUEST_FORWARD_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.interceptor.GET_request_forward_operations",
                "GET request operations forwarded to another Nexus instance");

        public static final MetricInfo MULTIPART_UPLOAD_REQUEST_PART_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.interceptor.MULTIPART_upload_request_part_throughput",
                "Throughput of MULTIPART upload request part");

        public static final MetricInfo MULTIPART_UPLOAD_REQUEST_PART_SIZE = new MetricInfo(
                PREFIX + "streamlet.interceptor.MULTIPART_upload_request_part_size",
                "Size of MULTIPART upload request part");

        public static final MetricInfo MULTIPART_UPLOAD_REQUEST_PART_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.interceptor.MULTIPART_upload_request_part_operations",
                "MULTIPART upload request part operations received");

        public static final MetricInfo MULTIPART_UPLOAD_COMPLETE_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.interceptor.MULTIPART_upload_complete_throughput",
                "Throughput of completing a MULTIPART upload");

        public static final MetricInfo MULTIPART_UPLOAD_COMPLETE_SIZE = new MetricInfo(
                PREFIX + "streamlet.interceptor.MULTIPART_upload_complete_size",
                "Size of a complete MULTIPART upload");

        public static final MetricInfo MULTIPART_UPLOAD_COMPLETE_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.interceptor.MULTIPART_upload_complete_operations",
                "Complete MULTIPART upload requests received");

        public static final MetricInfo METADATA_TAGS_UPDATE_LATENCY = new MetricInfo(
                PREFIX + "streamlet.interceptor.UPDATE_metadata_tags_latency",
                "Latency of update object metadata tags");

        public static final MetricInfo METADATA_TAGS_UPDATE_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.interceptor.UPDATE_metadata_tags_operations",
                "Counter for update object metadata tags operations");

        // Streamlet Execution
        public static final MetricInfo STREAMLET_POLICY_METADATA_RETRIEVAL_DURATION = new MetricInfo(
                        PREFIX + "streamlet.executor.policy_metadata_retrieval_duration",
                        "Duration of streamlet policy metadata retrieval execution");

        public static final MetricInfo STREAMLET_PIPELINE_BUILD_DURATION = new MetricInfo(
                        PREFIX + "streamlet.executor.streamlet_pipeline_execution_duration",
                        "Duration of streamlet pipeline building");

        public static final MetricInfo PUT_STREAMLET_PIPELINE_EXECUTION_LATENCY = new MetricInfo(
                        PREFIX + "streamlet.function.put_pipeline_execution_latency",
                        "Duration of executing a PUT streamlet pipeline");

        public static final MetricInfo PUT_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.function.put_pipeline_execution_throughput",
                "Throughput of executing a PUT streamlet pipeline");

        public static final MetricInfo GET_STREAMLET_PIPELINE_EXECUTION_LATENCY = new MetricInfo(
                PREFIX + "streamlet.function.get_pipeline_execution_latency",
                "Duration of executing a GET streamlet pipeline");

        public static final MetricInfo GET_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT = new MetricInfo(
                PREFIX + "streamlet.function.get_pipeline_execution_throughput",
                "Throughput of executing a GET streamlet pipeline");

        public static final MetricInfo PUT_STREAMLET_EXECUTION_LATENCY = new MetricInfo(
                PREFIX + "streamlet.function.put_execution_latency",
                "Duration of executing a PUT streamlet");

        public static final MetricInfo GET_STREAMLET_EXECUTION_LATENCY = new MetricInfo(
                PREFIX + "streamlet.function.get_execution_latency",
                "Duration of executing a GET streamlet");

        // Streamlet compilation
        public static final MetricInfo STREAMLET_INSTANTIATION_DURATION = new MetricInfo(
                PREFIX + "streamlet.function.instantiation_duration",
                "Duration of compiling and instantiating a Streamlet");

        // Stateful streamlets metadata access

        public static final MetricInfo STREAMLET_STATE_WRITE_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.function.WRITE_function_state_operations",
                "Counter for write operations of a streamlet state");

        public static final MetricInfo STREAMLET_STATE_READ_OPERATIONS = new MetricInfo(
                PREFIX + "streamlet.function.READ_function_state_operations",
                "Counter for read operations of a streamlet state");
}