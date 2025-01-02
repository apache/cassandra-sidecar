package org.apache.cassandra.sidecar.coordination;

/**
 * Class to encapsule Cassandra instance data
 */
public class CassandraInstance
{
    private final String token;
    private final String node;

    public CassandraInstance(String token, String node)
    {
        this.token = token;
        this.node = node;
    }

    public String token()
    {
        return token;
    }

    public String nodeName()
    {
        return node;
    }
}
