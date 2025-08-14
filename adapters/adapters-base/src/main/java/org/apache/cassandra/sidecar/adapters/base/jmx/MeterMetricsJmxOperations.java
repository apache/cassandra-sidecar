package org.apache.cassandra.sidecar.adapters.base.jmx;

/**
 * An interface that pulls meter metric methods from Cassandra JMX proxy.
 * Meter metrics track the rate of events occurring over time.
 */
public interface MeterMetricsJmxOperations {
    
    /**
     * Returns the total number of events that have occurred.
     * @return the total count of events
     */
    long getCount();

    /**
     * Returns the mean rate of events per second over the entire lifetime of the meter.
     * @return the mean rate in events per second
     */
    double getMeanRate();

    /**
     * Returns the one-minute exponentially-weighted moving average rate.
     * @return the one-minute rate in events per second
     */
    double getOneMinuteRate();

    /**
     * Returns the five-minute exponentially-weighted moving average rate.
     * @return the five-minute rate in events per second
     */
    double getFiveMinuteRate();

    /**
     * Returns the fifteen-minute exponentially-weighted moving average rate.
     * @return the fifteen-minute rate in events per second
     */
    double getFifteenMinuteRate();
}
