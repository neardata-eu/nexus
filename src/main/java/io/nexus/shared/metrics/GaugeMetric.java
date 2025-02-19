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

    public static double getMBps(long timeNanos, long dataBytes) {
        if (timeNanos <= 0) {
            throw new IllegalArgumentException("Time must be greater than zero.");
        }
        double timeSeconds = timeNanos / 1_000_000_000.0; // Convert nanoseconds to seconds
        double dataMB = dataBytes / (1024.0 * 1024.0); // Convert bytes to megabytes
        return dataMB / timeSeconds; // MBps
    }

}
