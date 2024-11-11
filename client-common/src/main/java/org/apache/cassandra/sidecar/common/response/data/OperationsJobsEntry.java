package org.apache.cassandra.sidecar.common.response.data;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Structure of the operations job instance within the list operations jobs API response
 */
public class OperationsJobsEntry
{
    public final UUID jobId;
    public final String status;
    public final String failureReason;
    public final String operation;

    /**
     * Constructs a {@link OperationsJobsEntry} object.
     */
    public OperationsJobsEntry(@JsonProperty("jobId") UUID jobId,
                               @JsonProperty("status") String status,
                               @JsonProperty("failureReason") String failureReason,
                               @JsonProperty("operation") String operation)
    {
        this.jobId = jobId;
        this.status = status;
        this.failureReason = failureReason;
        this.operation = operation;
    }
}
