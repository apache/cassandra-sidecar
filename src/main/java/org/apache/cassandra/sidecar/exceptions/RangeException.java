package org.apache.cassandra.sidecar.exceptions;

/**
 * Custom exception
 */
public class RangeException extends RuntimeException
{
    public RangeException()
    {
        super();
    }

    public RangeException(String msg)
    {
        super(msg);
    }

    public RangeException(Throwable cause)
    {
        super(cause);
    }
}
