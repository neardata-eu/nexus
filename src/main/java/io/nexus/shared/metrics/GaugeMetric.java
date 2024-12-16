package io.nexus.shared.metrics;

import io.nexus.shared.MetricInfo;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.snapshots.Unit;

/*
 * Generic gauge class for current measurements
 */
public class GaugeMetric implements Metric {
    private final Gauge gauge;

    public GaugeMetric(MetricInfo info) {
        // Using default inferred registry
        this.gauge = Gauge.builder().name(info.getName()).help(info.getDescription()).register();
    }

    public GaugeMetric(MetricInfo info, Unit unit) {
        this.gauge = Gauge.builder().name(info.getName()).help(info.getDescription()).unit(unit).register();
    }

    public void record(double value) {
        gauge.set(value);
    }

    @Override
    public String getId() {
        return gauge.getPrometheusName();
    }

}
