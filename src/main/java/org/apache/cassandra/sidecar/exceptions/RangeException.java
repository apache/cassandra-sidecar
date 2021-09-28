package org.apache.cassandra.sidecar.exceptions;

/**
 * Custom exception
 */
public class RangeException extends RuntimeException
{
    public RangeException(String msg)
    {
        super(msg);
    }
}
