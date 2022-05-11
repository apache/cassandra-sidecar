package org.apache.cassandra.sidecar.cluster.instance;

import java.util.List;

import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;

/**
 * Local implementation of InstanceMetadata.
 */
public class InstanceMetadataImpl implements InstanceMetadata
{
    private final int id;
    private final String host;
    private final int port;
    private final List<String> dataDirs;
    private final CQLSession session;
    private final CassandraAdapterDelegate delegate;

    public InstanceMetadataImpl(int id, String host, int port, List<String> dataDirs, CQLSession session,
                                CassandraVersionProvider versionProvider, int healthCheckFrequencyMillis)
    {
        this.id = id;
        this.host = host;
        this.port = port;
        this.dataDirs = dataDirs;

        this.session = new CQLSession(host, port, healthCheckFrequencyMillis);
        this.delegate = new CassandraAdapterDelegate(versionProvider, session, healthCheckFrequencyMillis);
    }

    public int id()
    {
        return id;
    }

    public String host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    public List<String> dataDirs()
    {
        return dataDirs;
    }

    public CQLSession session()
    {
        return session;
    }

    public CassandraAdapterDelegate delegate()
    {
        return delegate;
    }
}
