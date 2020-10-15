package org.apache.cassandra.sidecar.metrics.cdc;

/**
 * Interface to collecting metrics from the CDC reader.
 */
public interface CDCReaderMonitor
{
    void incSentSuccess();
    void incSentFailure();
    void reportCdcRawDirectorySizeInBytes(long dirSize);
}
