package org.apache.cassandra.sidecar.adapters.base.jmx;

public interface GaugeMetricsJmxOperations {
    /**
     * Retrieves the value of the metric of type {@link com.codahale.metrics.Gauge}
     * @return the value of the Gauge metric
     */
    Object getValue();
}
