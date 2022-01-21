package org.apache.cassandra.sidecar.cluster;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Local implementation of InstancesConfig.
 */
public class InstancesConfigImpl implements InstancesConfig
{
    private final Map<Integer, InstanceMetadata> idToInstanceMetas;
    private final Map<String, InstanceMetadata> hostToInstanceMetas;
    private final List<InstanceMetadata> instanceMetas;

    public InstancesConfigImpl(List<InstanceMetadata> instanceMetas)
    {
        this.idToInstanceMetas = instanceMetas.stream()
                                              .collect(Collectors.toMap(InstanceMetadata::id,
                                                                        instanceMeta -> instanceMeta));
        this.hostToInstanceMetas = instanceMetas.stream()
                                                .collect(Collectors.toMap(InstanceMetadata::host,
                                                                          instanceMeta -> instanceMeta));
        this.instanceMetas = instanceMetas;
    }

    public List<InstanceMetadata> instances()
    {
        return instanceMetas;
    }

    public InstanceMetadata instanceFromId(int id)
    {
        if (!idToInstanceMetas.containsKey(id))
        {
            throw new IllegalArgumentException("Instance id " + id + " not found");
        }
        return idToInstanceMetas.get(id);
    }

    public InstanceMetadata instanceFromHost(String host)
    {
        if (!hostToInstanceMetas.containsKey(host))
        {
            throw new IllegalArgumentException("Instance with host address " + host + " not found");
        }
        return hostToInstanceMetas.get(host);
    }
}
