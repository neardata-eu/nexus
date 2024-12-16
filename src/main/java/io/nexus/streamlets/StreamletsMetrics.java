package io.nexus.streamlets;

import io.nexus.shared.MetricNames;
import io.nexus.shared.metrics.TimerMetric;

/*
 * Defining all the metrics regarding streamlets operations
 */
public class StreamletsMetrics {

        public static final TimerMetric PUT_REQUEST_TIMER = new TimerMetric(
                        MetricNames.PUT_REQUEST_INTERCEPTION_DURATION);
        public static final TimerMetric GET_REQUEST_TIMER = new TimerMetric(
                        MetricNames.GET_REQUEST_INTERCEPTION_DURATION);
        public static final TimerMetric MULTIPART_REQUEST_TIMER = new TimerMetric(
                        MetricNames.MULTIPART_REQUEST_INTERCEPTION_DURATION);

        public static final TimerMetric MULTIPART_EVENT_TIMER = new TimerMetric(
                        MetricNames.MULTIPART_EVENT_INTERCEPTION_DURATION);

        public static final TimerMetric POLICY_RETRIEVAL_TIMER = new TimerMetric(
                        MetricNames.STREAMLET_POLICY_METADATA_RETRIEVAL_DURATION);
        public static final TimerMetric PIPELINE_BUILD_TIMER = new TimerMetric(
                        MetricNames.STREAMLET_PIPELINE_BUILD_DURATION);

        public static final TimerMetric NO_OP_TIMER = new TimerMetric(MetricNames.STREAMLET_FUNCTION_NO_OP_DURATION);

}
