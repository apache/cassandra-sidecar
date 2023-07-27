package org.apache.cassandra.testing;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.UpgradeableCluster;

/**
 * A Cassandra Test Context implementation that allows advanced cluster configuration before cluster creation
 * by providing access to the cluster builder.
 */
public class ConfigurableCassandraTestContext extends AbstractCassandraTestContext
{
    public static final String BUILT_CLUSTER_CANNOT_BE_CONFIGURED_ERROR =
        "Cannot configure a cluster after it is built. Please set the buildCluster annotation attribute to false, " +
        "and do not call `getCluster` before calling this method.";

    private final UpgradeableCluster.Builder builder;

    public ConfigurableCassandraTestContext(SimpleCassandraVersion version,
                                            UpgradeableCluster.Builder builder)
    {
        super(version);
        this.builder = builder;
    }

    public UpgradeableCluster configureCluster(Consumer<UpgradeableCluster.Builder> configurator) throws IOException
    {
        if (cluster != null)
        {
            throw new IllegalStateException(BUILT_CLUSTER_CANNOT_BE_CONFIGURED_ERROR);
        }
        configurator.accept(builder);
        cluster = builder.createWithoutStarting();
        return cluster;
    }

    public UpgradeableCluster configureAndStartCluster(Consumer<UpgradeableCluster.Builder> configurator)
        throws IOException
    {
        cluster = configureCluster(configurator);
        cluster.startup();
        return cluster;
    }

    @Override
    public String toString()
    {
        return "ConfigurableCassandraTestContext{" +
               ", version=" + version +
               ", builder=" + builder +
               '}';
    }
}
