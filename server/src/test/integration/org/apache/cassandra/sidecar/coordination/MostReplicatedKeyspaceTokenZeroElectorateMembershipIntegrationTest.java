/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.coordination;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.vdurmont.semver4j.Semver;
import io.vertx.core.Vertx;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.InstancesMetadataImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.instance.InstanceHealthMetrics;
import org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.testing.TestVersion;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.sidecar.coordination.ClusterLeaseClaimTaskIntegrationTest.buildContactList;
import static org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext.cassandraVersionProvider;
import static org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext.tryGetIntConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the {@link MostReplicatedKeyspaceTokenZeroElectorateMembership} class
 */
@Tag("heavy")
class MostReplicatedKeyspaceTokenZeroElectorateMembershipIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MostReplicatedKeyspaceTokenZeroElectorateMembershipIntegrationTest.class);
    private static final SidecarConfigurationImpl CONFIG = new SidecarConfigurationImpl();
    Vertx vertx = Vertx.vertx();
    DriverUtils driverUtils = new DriverUtils();
    CassandraVersionProvider cassandraVersionProvider = cassandraVersionProvider(DnsResolver.DEFAULT);

    @ParameterizedTest(name = "{index} => version {0}")
    @MethodSource("org.apache.cassandra.testing.TestVersionSupplier#testVersions")
    void test(TestVersion version) throws IOException
    {
        Versions versions = Versions.find();
        assertThat(versions).as("No dtest jar versions found").isNotNull();
        Versions.Version requestedVersion = versions.getLatest(new Semver(version.version(), Semver.SemverType.LOOSE));

        // Spin up a 12 node cluster with 2 DCs
        try (AbstractCluster<?> cluster = UpgradeableCluster.build()
                                                            .withDynamicPortAllocation(true) // to allow parallel test runs
                                                            .withVersion(requestedVersion)
                                                            .withDC("dc0", 6)
                                                            .withDC("dc1", 6)
                                                            .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                                                        .with(Feature.JMX)
                                                                                        .with(Feature.NETWORK)
                                                                                        .with(Feature.GOSSIP))
                                                            .start())
        {
            initializeSchema(cluster);
            runTestScenario(cluster);
        }
    }

    private void runTestScenario(AbstractCluster<?> cluster)
    {
        List<MostReplicatedKeyspaceTokenZeroElectorateMembership> memberships = buildElectorateMembershipPerCassandraInstance(cluster);
        // When there are no user keyspaces, we default to the sidecar_internal keyspace
        // and therefore guaranteeing that we have at least one keyspace to use for the
        // determination of the membership, and that's why we expect the membership count
        // to be one, even if we have not created user keyspaces yet.
        assertMembership(memberships, 1);

        // Now let's create keyspaces with RF 1-3 replicated in a single DC and validate
        String dc0 = "dc0";
        for (int rf = 1; rf <= 3; rf++)
        {
            cluster.schemaChange(String.format("CREATE KEYSPACE ks_dc0_%d WITH REPLICATION={'class':'NetworkTopologyStrategy','%s':%d}", rf, dc0, rf));
            // introduce delay until schema change information propagates
            sleepUninterruptibly(10, TimeUnit.SECONDS);
            assertMembership(memberships, rf);
        }

        // Now let's create keyspaces with RF 1-4 replicated in DC2 and validate
        // that we only increase the membership count once the keyspace in DC2
        // has a higher replication factor than the keyspaces created in the first DC
        String dc1 = "dc1";
        for (int rf = 1; rf <= 4; rf++)
        {
            cluster.schemaChange(String.format("CREATE KEYSPACE ks_dc1_%d WITH REPLICATION={'class':'NetworkTopologyStrategy','%s':%d}", rf, dc1, rf));
            // introduce delay until schema change information propagates
            sleepUninterruptibly(10, TimeUnit.SECONDS);
            assertMembership(memberships, Math.max(3, rf));
        }

        // Now let's create a keyspace with RF=3 replicated across both DCs
        cluster.schemaChange("CREATE KEYSPACE ks_all_3 WITH REPLICATION={'class':'NetworkTopologyStrategy','replication_factor':3}");
        // introduce delay until schema change information propagates
        sleepUninterruptibly(10, TimeUnit.SECONDS);
        // We expect the same instances in the existing keyspaces to own token 0 as the new keyspace
        // so a total of 6 instances own token 0, 3 on each DC.
        assertMembership(memberships, 6);
    }

    static void assertMembership(List<MostReplicatedKeyspaceTokenZeroElectorateMembership> memberships, int expectedElectorateSize)
    {
        int localElectorateCount = 0;
        for (MostReplicatedKeyspaceTokenZeroElectorateMembership membership : memberships)
        {
            boolean shouldParticipate = membership.isMember();
            if (shouldParticipate)
            {
                localElectorateCount++;
            }
        }
        assertThat(localElectorateCount).as("We expect %s instances of TokenZeroElectorateMembership to participate in the election", expectedElectorateSize)
                                        .isEqualTo(expectedElectorateSize);
    }

    List<MostReplicatedKeyspaceTokenZeroElectorateMembership> buildElectorateMembershipPerCassandraInstance(AbstractCluster<?> cluster)
    {
        MetricRegistryFactory metricRegistryProvider = new MetricRegistryFactory("cassandra_sidecar",
                                                                                 Collections.emptyList(),
                                                                                 Collections.emptyList());

        List<MostReplicatedKeyspaceTokenZeroElectorateMembership> result = new ArrayList<>();
        for (IInstance instance : cluster)
        {
            List<InetSocketAddress> address = buildContactList(instance);
            CQLSessionProvider sessionProvider =
            new CQLSessionProviderImpl(address, address, 500, instance.config().localDatacenter(), 0, SharedExecutorNettyOptions.INSTANCE);
            InstancesMetadata instancesMetadata = buildInstancesMetadata(instance, sessionProvider, metricRegistryProvider);
            InstanceMetadataFetcher instanceMetadataFetcher = new InstanceMetadataFetcher(instancesMetadata);
            result.add(new MostReplicatedKeyspaceTokenZeroElectorateMembership(instanceMetadataFetcher, sessionProvider, CONFIG));
        }
        return result;
    }

    private InstancesMetadata buildInstancesMetadata(IInstance instance,
                                                     CQLSessionProvider sessionProvider,
                                                     MetricRegistryFactory metricRegistryProvider)
    {
        IInstanceConfig config = instance.config();
        MetricRegistry instanceSpecificRegistry = metricRegistryProvider.getOrCreate(config.num());
        String hostName = JMXUtil.getJmxHost(config);
        int nativeTransportPort = tryGetIntConfig(config, "native_transport_port", 9042);
        String[] dataDirectories = (String[]) config.get("data_file_directories");


        JmxClient jmxClient = JmxClient.builder()
                                       .host(hostName)
                                       .port(config.jmxPort())
                                       .connectionMaxRetries(20)
                                       .connectionRetryDelay(MillisecondBoundConfiguration.parse("500ms"))
                                       .build();

        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
                                                                         config.num(),
                                                                         cassandraVersionProvider,
                                                                         sessionProvider,
                                                                         jmxClient,
                                                                         driverUtils,
                                                                         "1.0-TEST",
                                                                         hostName,
                                                                         nativeTransportPort,
                                                                         new InstanceHealthMetrics(instanceSpecificRegistry));

        // we need to establish CQL + JMX connections required by the implementation
        // so run the healthcheck
        delegate.healthCheck();

        // equivalent of one Sidecar instance managing a single Cassandra instance
        List<InstanceMetadata> metadata =
        Collections.singletonList(InstanceMetadataImpl.builder()
                                                      .id(config.num())
                                                      .host(config.broadcastAddress().getAddress().getHostAddress())
                                                      .port(nativeTransportPort)
                                                      .dataDirs(Arrays.asList(dataDirectories))
                                                      .cdcDir(config.getString("cdc_raw_directory"))
                                                      .commitlogDir(config.getString("commitlog_directory"))
                                                      .hintsDir(config.getString("hints_directory"))
                                                      .savedCachesDir(config.getString("saved_caches_directory"))
                                                      .delegate(delegate)
                                                      .metricRegistry(instanceSpecificRegistry)
                                                      .build());
        return new InstancesMetadataImpl(metadata, DnsResolver.DEFAULT);
    }

    void initializeSchema(AbstractCluster<?> cluster)
    {
        SchemaKeyspaceConfiguration config = CONFIG.serviceConfiguration().schemaKeyspaceConfiguration();
        String createKeyspaceStatement = String.format("CREATE KEYSPACE %s WITH REPLICATION = %s ;",
                                                       config.keyspace(), config.createReplicationStrategyString());
        cluster.schemaChange(createKeyspaceStatement);
        LOGGER.info("Creating keyspace with DDL: {}", createKeyspaceStatement);
    }
}
