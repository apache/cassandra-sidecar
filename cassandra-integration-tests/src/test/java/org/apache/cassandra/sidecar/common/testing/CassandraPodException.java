package org.apache.cassandra.sidecar.common.testing;

/**
 * Misc exception to be thrown when we don't know what's actually wrong
 */
public class CassandraPodException extends Exception
{
    public CassandraPodException(String message)
    {
        super(message);
    }
}
