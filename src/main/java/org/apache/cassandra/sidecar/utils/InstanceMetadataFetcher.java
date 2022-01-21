package org.apache.cassandra.sidecar.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;

/**
 * To fetch instance information according to instance id provided.
 * By default returns 1st instance's information
 */
@Singleton
public class InstanceMetadataFetcher
{
    private final InstancesConfig instancesConfig;

    @Inject
    public InstanceMetadataFetcher(InstancesConfig instancesConfig)
    {
        this.instancesConfig = instancesConfig;
    }

    public CassandraAdapterDelegate getDelegate(String host)
    {
        return host == null
               ? getFirstInstance().delegate()
               : instancesConfig.instanceFromHost(host).delegate();
    }

    public CassandraAdapterDelegate getDelegate(Integer instanceId)
    {
        return instanceId == null
               ? getFirstInstance().delegate()
               : instancesConfig.instanceFromId(instanceId).delegate();
    }

    public FilePathBuilder getPathBuilder(String host)
    {
        return host == null
               ? getFirstInstance().pathBuilder()
               : instancesConfig.instanceFromHost(host).pathBuilder();
    }

    public FilePathBuilder getPathBuilder(Integer instanceId)
    {
        return instanceId == null
               ? getFirstInstance().pathBuilder()
               : instancesConfig.instanceFromId(instanceId).pathBuilder();
    }

    private InstanceMetadata getFirstInstance()
    {
        if (instancesConfig.instances().isEmpty())
        {
            throw new IllegalStateException("There are no instances configured!");
        }
        return instancesConfig.instances().get(0);
    }
}
