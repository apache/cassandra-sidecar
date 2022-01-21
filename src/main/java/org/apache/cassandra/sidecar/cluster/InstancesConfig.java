package org.apache.cassandra.sidecar.cluster;

import java.util.List;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Maintains metadata of instances maintained by Sidecar.
 */
public interface InstancesConfig
{
    /**
     * returns metadata of instances owned by the sidecar
     */
    List<InstanceMetadata> instances();

    /**
     * Lookup instance metadata by id.
     * @param id instance's id
     * @return instance meta information
     */
    InstanceMetadata instanceFromId(final int id);

    /**
     * Lookup instance metadata by host name.
     * @param host host address of instance
     * @return instance meta information
     */
    InstanceMetadata instanceFromHost(final String host);
}
