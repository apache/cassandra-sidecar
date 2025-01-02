package org.apache.cassandra.sidecar.health;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarPeerHealthConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarPeerHealthConfigurationImpl;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.InnerDcTokenAdjacentPeerTestProvider.TestSidecarHostInfo;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SidecarPeerDownDetectorIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{

    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarPeerDownDetectorIntegrationTest.class);

    List<TestSidecarHostInfo> sidecarServerList = new ArrayList<>();

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return new ClusterBuilderConfiguration().nodesPerDc(3);
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        Supplier<List<TestSidecarHostInfo>> supplier = () -> sidecarServerList;
        PeersModule peersModule = new PeersModule(supplier);
        for (IInstance instance : cluster)
        {
            // Provider de una lista de Sidecar servers
            LOGGER.info("Starting Sidecar instance for Cassandra instance {}",
                        instance.config().num());
            Server server = startSidecarWithInstances(List.of(instance), peersModule);
            sidecarServerList.add(new TestSidecarHostInfo(instance, server, server.actualPort()));
        }

        assertThat(sidecarServerList.size()).as("Each Cassandra Instance will be managed by a single Sidecar instance")
                                            .isEqualTo(cluster.size());


        // assign the server to the first instance
        server = sidecarServerList.get(0).getServer();
    }

    class PeersModule extends AbstractModule
    {
        Supplier<List<TestSidecarHostInfo>> supplier;
        public PeersModule(Supplier<List<TestSidecarHostInfo>> supplier)
        {
            this.supplier = supplier;
        }

        @Provides
        @Singleton
        @Named("sidecarInstanceSupplier")
        public Supplier<List <TestSidecarHostInfo>> supplier()
        {
            return supplier;
        }

        @Provides
        @Singleton
        public SidecarPeerHealthConfiguration sidecarPeerHealthConfiguration()
        {
            return new SidecarPeerHealthConfigurationImpl(true,
                                                          new MillisecondBoundConfiguration(1, TimeUnit.SECONDS),
                                                          1,
                                                          new MillisecondBoundConfiguration(500, TimeUnit.MILLISECONDS));
        }
    }

    void stopSidecarInstanceForTest(int instanceId) throws Exception
    {
        assertThat(sidecarServerList).isNotEmpty();
        TestSidecarHostInfo server = sidecarServerList.get(instanceId);
        server.getServer().stop().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    void startSidecarInstanceForTest(int instanceId) throws Exception
    {
        assertThat(sidecarServerList).isNotEmpty();
        TestSidecarHostInfo server = sidecarServerList.get(instanceId);
        server.getServer().start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> {
            ServiceConfiguration conf;
            if (sidecarServerList.isEmpty())
            {
                // As opposed to the base class, this binds the host to a specific interface (localhost)
                conf = ServiceConfigurationImpl.builder()
                                               .host("localhost")
                                               .port(0) // let the test find an available port
                                               .build();
            }
            else
            {
                // Use the same port number for all Sidecar instances that we bring up. We use the port
                // bound for the first instance, but we bind it to a different interface (localhost2, localhost3)
                conf = ServiceConfigurationImpl.builder()
                                               .host("localhost" + (sidecarServerList.size() + 1))
                                               .port(sidecarServerList.get(0).getServer().actualPort())
                                               .build();
            }
            builder.serviceConfiguration(conf);

            return builder;
        };
    }

    @Test
    void oneBuddyDownTest() throws Exception
    {
        HttpResponse<Buffer> response = getBlocking(trustedClient().get(server.actualPort(), "localhost", "/api/v1/peers/__health")
                                                                   .send());
        assertTrue(response.bodyAsJsonObject().isEmpty());
        Thread.sleep(10000);
        response = getBlocking(trustedClient().get(server.actualPort(), "localhost", "/api/v1/peers/__health")
                                              .send());

        assertEquals("OK", response.bodyAsJsonObject().getJsonObject("localhost2").getString("status"));

        stopSidecarInstanceForTest(1);

        Thread.sleep(10000);

        response = getBlocking(trustedClient().get(server.actualPort(), "localhost", "/api/v1/peers/__health")
                                              .send());

        assertEquals("DOWN", response.bodyAsJsonObject().getJsonObject("localhost2").getString("status"));

        startSidecarInstanceForTest(1);

        Thread.sleep(10000);

        response = getBlocking(trustedClient().get(server.actualPort(), "localhost", "/api/v1/peers/__health")
                                              .send());

        assertEquals("OK", response.bodyAsJsonObject().getJsonObject("localhost2").getString("status"));
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace("cdc", Map.of("datacenter1", 3));
        createTestTable(new QualifiedName("cdc", "test"), "CREATE TABLE %s (id text PRIMARY KEY, name text) WITH cdc=true;");
    }
}

