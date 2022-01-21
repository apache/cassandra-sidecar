package org.apache.cassandra.sidecar.models;

/**
 * Stores information needed to identify a SStable component.
 */
public class ComponentInfo
{
    private final String keyspace;
    private final String table;
    private final String snapshot;
    private final String component;

    public ComponentInfo(String keyspace, String table, String snapshot, String component)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.snapshot = snapshot;
        this.component = component;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
    }

    public String getSnapshot()
    {
        return snapshot;
    }

    public String getComponent()
    {
        return component;
    }
}
