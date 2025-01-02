package org.apache.cassandra.sidecar.testing;

import java.util.List;
import java.util.function.Supplier;

import com.datastax.driver.core.Host;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.coordination.CassandraClientTokenRingProvider;
import org.apache.cassandra.sidecar.coordination.InnerDcTokenAdjacentPeerProvider;
import org.apache.cassandra.sidecar.server.Server;

public class InnerDcTokenAdjacentPeerTestProvider extends InnerDcTokenAdjacentPeerProvider
{
    private final Supplier<List<TestSidecarHostInfo>> sidecarServerSupplier;

    public InnerDcTokenAdjacentPeerTestProvider(InstancesMetadata instancesMetadata,
                                                CassandraClientTokenRingProvider cassandraClientTokenRingProvider,
                                                ServiceConfiguration serviceConfiguration,
                                                DnsResolver dnsResolver,
                                                Supplier<List<TestSidecarHostInfo>> sidecarServerSupplier)
    {
        super(instancesMetadata, cassandraClientTokenRingProvider, serviceConfiguration, dnsResolver);
        this.sidecarServerSupplier = sidecarServerSupplier;
    }

    @Override
    protected int sidecarServicePort(Host host)
    {
        return sidecarServerSupplier.get().stream()
                                    .filter(s -> s.instance.broadcastAddress().getHostName().equals(host.getBroadcastAddress().getHostName())).findAny().orElseThrow().getPort();
    }

    public static class TestSidecarHostInfo
    {
        IInstance instance;
        Server sidecarServer;
        int port;

        public TestSidecarHostInfo(IInstance instance, Server sidecarServer, int port)
        {
            this.instance = instance;
            this.sidecarServer = sidecarServer;
            this.port = port;
        }

        public IInstance getInstance()
        {
            return instance;
        }

        public Server getServer()
        {
            return sidecarServer;
        }

        public int getPort()
        {
            return port;
        }
    }
}
