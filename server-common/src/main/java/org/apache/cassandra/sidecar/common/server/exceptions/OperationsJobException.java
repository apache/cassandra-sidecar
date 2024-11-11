package org.apache.cassandra.sidecar.common.server.exceptions;

import java.util.UUID;

/**
 * Exception thrown when a operations job conflict is detected
 */
public class OperationsJobException extends RuntimeException
{
    private final UUID headerValue;
    public OperationsJobException(String message, UUID jobId)
    {
        super(message);
        this.headerValue = jobId;
    }

    public OperationsJobException(String message)
    {
        super(message);
        this.headerValue = null;
    }

    public UUID getHeaderValue()
    {
        return headerValue;
    }
}
