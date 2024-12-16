package io.nexus.shared.metrics;

import io.nexus.shared.MetricInfo;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.snapshots.Unit;

/*
 * Generic timer class for measuring execution duration
 */
public class TimerMetric implements Metric {

    private final Histogram duration;

    public TimerMetric(MetricInfo info) {
        // Using default inferred registry
        this.duration = Histogram.builder().name(info.getName()).help(info.getDescription()).unit(Unit.SECONDS)
                .register();
    }

    public void record(long amount) {
        duration.observe(Unit.nanosToSeconds(amount));
    }

    @Override
    public String getId() {
        return duration.getPrometheusName();
    }

}