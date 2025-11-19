package org.apache.cassandra.sidecar.common.data;

/**
 * Status values for compaction stop operations
 */
public enum CompactionStopStatus
{
    /**
     * Compaction stop request submitted to Cassandra - ongoing compactions now stopping
     */
    SUBMITTED,

    /**
     * Compaction stop request failed
     */
    FAILED;
}
