package io.nexus.streamlets;

import io.nexus.shared.MetricNames;
import io.nexus.shared.metrics.CounterMetric;
import io.nexus.shared.metrics.GaugeMetric;
import io.nexus.shared.metrics.TimerMetric;

/*
 * Defining all the metrics regarding streamlets operations
 */
public class StreamletsMetrics {

        // Request Interception
        public static final TimerMetric PUT_REQUEST_INTERCEPTION_DURATION_TIMER = new TimerMetric(
                        MetricNames.PUT_REQUEST_INTERCEPTION_DURATION);
        public static final TimerMetric GET_REQUEST_INTERCEPTION_DURATION_TIMER = new TimerMetric(
                        MetricNames.GET_REQUEST_INTERCEPTION_DURATION);
        public static final TimerMetric INITIATE_MULTIPART_REQUEST_INTERCEPTION_DURATION_TIMER = new TimerMetric(
                        MetricNames.INITIATE_MULTIPART_REQUEST_INTERCEPTION_DURATION);
        public static final TimerMetric ABORT_MULTIPART_REQUEST_INTERCEPTION_DURATION_TIMER = new TimerMetric(
                MetricNames.ABORT_MULTIPART_REQUEST_INTERCEPTION_DURATION);

        // Request IO
        public static final GaugeMetric PUT_REQUEST_STORAGE_THROUGHPUT_GAUGE = new GaugeMetric(
                MetricNames.PUT_REQUEST_STORAGE_THROUGHPUT);
        public static final GaugeMetric PUT_REQUEST_STORAGE_SIZE_GAUGE  = new GaugeMetric(
                MetricNames.PUT_REQUEST_STORAGE_SIZE);
        public static final CounterMetric PUT_REQUEST_STORAGE_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.PUT_REQUEST_STORAGE_OPERATIONS);
        public static final GaugeMetric PUT_REQUEST_FORWARD_THROUGHPUT_GAUGE  = new GaugeMetric(
                MetricNames.PUT_REQUEST_FORWARD_THROUGHPUT);
        public static final GaugeMetric PUT_REQUEST_FORWARD_SIZE_GAUGE = new GaugeMetric(
                MetricNames.PUT_REQUEST_FORWARD_SIZE);
        public static final CounterMetric PUT_REQUEST_FORWARD_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.PUT_REQUEST_FORWARD_OPERATIONS);
        public static final GaugeMetric GET_REQUEST_STORAGE_THROUGHPUT_GAUGE = new GaugeMetric(
                MetricNames.GET_REQUEST_STORAGE_THROUGHPUT);
        public static final GaugeMetric GET_REQUEST_STORAGE_SIZE_GAUGE  = new GaugeMetric(
                MetricNames.GET_REQUEST_STORAGE_SIZE);
        public static final CounterMetric GET_REQUEST_STORAGE_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.GET_REQUEST_STORAGE_OPERATIONS);
        public static final GaugeMetric GET_REQUEST_FORWARD_THROUGHPUT_GAUGE  = new GaugeMetric(
                MetricNames.GET_REQUEST_FORWARD_THROUGHPUT);
        public static final GaugeMetric GET_REQUEST_FORWARD_SIZE_GAUGE = new GaugeMetric(
                MetricNames.GET_REQUEST_FORWARD_SIZE);
        public static final CounterMetric GET_REQUEST_FORWARD_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.GET_REQUEST_FORWARD_OPERATIONS);
        public static final GaugeMetric MULTIPART_UPLOAD_REQUEST_PART_THROUGHPUT_GAUGE  = new GaugeMetric(
                MetricNames.MULTIPART_UPLOAD_REQUEST_PART_THROUGHPUT);
        public static final GaugeMetric MULTIPART_UPLOAD_REQUEST_PART_SIZE_GAUGE = new GaugeMetric(
                MetricNames.MULTIPART_UPLOAD_REQUEST_PART_SIZE);
        public static final CounterMetric MULTIPART_UPLOAD_REQUEST_PART_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.MULTIPART_UPLOAD_REQUEST_PART_OPERATIONS);
        public static final GaugeMetric MULTIPART_UPLOAD_COMPLETE_THROUGHPUT_GAUGE  = new GaugeMetric(
                MetricNames.MULTIPART_UPLOAD_COMPLETE_THROUGHPUT);
        public static final GaugeMetric MULTIPART_UPLOAD_COMPLETE_SIZE_GAUGE = new GaugeMetric(
                MetricNames.MULTIPART_UPLOAD_COMPLETE_SIZE);
        public static final CounterMetric MULTIPART_UPLOAD_COMPLETE_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.MULTIPART_UPLOAD_COMPLETE_OPERATIONS);
        public static final TimerMetric METADATA_TAGS_UPDATE_LATENCY_TIMER = new TimerMetric(
                MetricNames.METADATA_TAGS_UPDATE_LATENCY);
        public static final CounterMetric METADATA_TAGS_UPDATE_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.METADATA_TAGS_UPDATE_OPERATIONS);

        // Metadata metrics
        public static final TimerMetric POLICY_RETRIEVAL_TIMER = new TimerMetric(
                        MetricNames.STREAMLET_POLICY_METADATA_RETRIEVAL_DURATION);
        public static final TimerMetric PIPELINE_BUILD_TIMER = new TimerMetric(
                        MetricNames.STREAMLET_PIPELINE_BUILD_DURATION);
        public static final CounterMetric STREAMLET_STATE_WRITE_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.STREAMLET_STATE_WRITE_OPERATIONS);
        public static final CounterMetric STREAMLET_STATE_READ_OPERATIONS_COUNTER  = new CounterMetric(
                MetricNames.STREAMLET_STATE_READ_OPERATIONS);

        // Streamlet processing metrics
        public static final TimerMetric PUT_STREAMLET_PIPELINE_EXECUTION_LATENCY_TIMER = new TimerMetric(
                        MetricNames.PUT_STREAMLET_PIPELINE_EXECUTION_LATENCY);
        public static final GaugeMetric PUT_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT_GAUGE = new GaugeMetric(
                MetricNames.PUT_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT);
        public static final TimerMetric GET_STREAMLET_PIPELINE_EXECUTION_LATENCY_TIMER = new TimerMetric(
                MetricNames.GET_STREAMLET_PIPELINE_EXECUTION_LATENCY);
        public static final GaugeMetric GET_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT_GAUGE = new GaugeMetric(
                MetricNames.GET_STREAMLET_PIPELINE_EXECUTION_THROUGHPUT);
        // TODO: Add the capability to add "tags" to Timers, Gauges, and Counters so we can filter by Streamlet.
        public static final TimerMetric PUT_STREAMLET_EXECUTION_LATENCY_TIMER = new TimerMetric(
                MetricNames.PUT_STREAMLET_EXECUTION_LATENCY);
        public static final TimerMetric GET_STREAMLET_EXECUTION_LATENCY_TIMER = new TimerMetric(
                MetricNames.GET_STREAMLET_EXECUTION_LATENCY);

        // Streamlet compilation and instantiation
        public static final TimerMetric STREAMLET_INSTANTIATION_DURATION_TIMER = new TimerMetric(
                MetricNames.STREAMLET_INSTANTIATION_DURATION);
}
