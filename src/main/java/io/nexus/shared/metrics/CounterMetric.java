package io.nexus.shared.metrics;

import io.nexus.shared.MetricInfo;
import io.prometheus.metrics.core.metrics.Counter;

/*
 * Generic counter class for incrementals
 */

public class CounterMetric implements Metric {

    private final Counter counter;

    public CounterMetric(MetricInfo info) {
        this.counter = Counter.builder().name(info.getName()).help(info.getDescription()).register();
    }

    public void incrementCounter() {
        this.counter.inc();
    }

    public void incrementCounter(double num) {
        this.counter.inc(num);
    }

    @Override
    public String getId() {
        return counter.getPrometheusName();
    }

}