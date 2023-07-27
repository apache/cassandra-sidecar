package org.apache.cassandra.testing;

import org.apache.cassandra.distributed.UpgradeableCluster;

/**
 * The base class for all CassandraTestContext implementations
 */
public abstract class AbstractCassandraTestContext implements AutoCloseable
{
    public final SimpleCassandraVersion version;
    protected UpgradeableCluster cluster;

    public AbstractCassandraTestContext(SimpleCassandraVersion version, UpgradeableCluster cluster)
    {
        this.version = version;
        this.cluster = cluster;
    }

    public AbstractCassandraTestContext(SimpleCassandraVersion version)
    {
        this.version = version;
    }

    public void close() throws Exception
    {
        if (cluster != null)
        {
            cluster.close();
        }
    }
}
