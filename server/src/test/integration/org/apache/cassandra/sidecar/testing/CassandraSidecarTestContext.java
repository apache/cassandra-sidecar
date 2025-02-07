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

package org.apache.cassandra.sidecar.testing;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Session;
import io.vertx.core.Vertx;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.impl.AbstractClusterUtils;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.adapters.cassandra41.Cassandra41Factory;
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
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.instance.InstanceHealthMetrics;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.AbstractCassandraTestContext;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Passed to integration tests.
 */
public class CassandraSidecarTestContext implements AutoCloseable
{
    public final SimpleCassandraVersion version;
    private final MetricRegistryFactory metricRegistryProvider = new MetricRegistryFactory("cassandra_sidecar",
                                                                                           Collections.emptyList(),
                                                                                           Collections.emptyList());
    private final CassandraVersionProvider versionProvider;
    private final DnsResolver dnsResolver;
    private final AbstractCassandraTestContext abstractCassandraTestContext;
    private final Vertx vertx;
    private final List<InstancesMetadataListener> instancesMetadataListeners;
    // array of nodeNums that are 1-based
    private int[] instancesToManage = null;
    public InstancesMetadata instancesMetadata;
    private List<JmxClient> jmxClients;
    private CQLSessionProvider sessionProvider;
    private String username = "cassandra";
    private String password = "cassandra";
    private SslConfiguration sslConfiguration;

    private CassandraSidecarTestContext(Vertx vertx,
                                        AbstractCassandraTestContext abstractCassandraTestContext,
                                        SimpleCassandraVersion version,
                                        CassandraVersionProvider versionProvider,
                                        DnsResolver dnsResolver,
                                        int[] instancesToManage,
                                        SslConfiguration sslConfiguration)
    {
        this.vertx = vertx;
        this.instancesToManage = instancesToManage;
        this.instancesMetadataListeners = new ArrayList<>();
        this.abstractCassandraTestContext = abstractCassandraTestContext;
        this.version = version;
        this.versionProvider = versionProvider;
        this.dnsResolver = dnsResolver;
        this.sslConfiguration = sslConfiguration;
    }

    public static CassandraSidecarTestContext from(Vertx vertx,
                                                   AbstractCassandraTestContext cassandraTestContext,
                                                   DnsResolver dnsResolver,
                                                   int[] instancesToManage,
                                                   SslConfiguration sslConfiguration)
    {
        org.apache.cassandra.testing.SimpleCassandraVersion rootVersion = cassandraTestContext.version;
        SimpleCassandraVersion versionParsed = SimpleCassandraVersion.create(rootVersion.major,
                                                                             rootVersion.minor,
                                                                             rootVersion.patch);
        CassandraVersionProvider versionProvider = cassandraVersionProvider(dnsResolver);
        return new CassandraSidecarTestContext(vertx,
                                               cassandraTestContext,
                                               versionParsed,
                                               versionProvider,
                                               dnsResolver,
                                               instancesToManage,
                                               sslConfiguration);
    }

    public static CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver)
    {
        DriverUtils driverUtils = new DriverUtils();
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, driverUtils))
               .add(new Cassandra41Factory(dnsResolver, driverUtils))
               .build();
    }

    public static int tryGetIntConfig(IInstanceConfig config, String configName, int defaultValue)
    {
        try
        {
            return config.getInt(configName);
        }
        catch (NullPointerException npe)
        {
            return defaultValue;
        }
    }

    public void registerInstanceConfigListener(InstancesMetadataListener listener)
    {
        this.instancesMetadataListeners.add(listener);
    }

    public AbstractCassandraTestContext cassandraTestContext()
    {
        return abstractCassandraTestContext;
    }

    public boolean isClusterBuilt()
    {
        return abstractCassandraTestContext.cluster() != null;
    }

    public UpgradeableCluster cluster()
    {
        UpgradeableCluster cluster = abstractCassandraTestContext.cluster();
        if (cluster == null)
        {
            throw new RuntimeException("The cluster must be built before it can be used");
        }
        return cluster;
    }

    public void setInstancesToManage(int... instancesToManage)
    {
        this.instancesToManage = instancesToManage;
        refreshInstancesMetadata();
    }

    public void setUsernamePassword(String username, String password)
    {
        this.username = username;
        this.password = password;
        refreshInstancesMetadata();
    }

    public synchronized InstancesMetadata instancesMetadata()
    {
        if (instancesMetadata == null)
        {
            return refreshInstancesMetadata();
        }
        return this.instancesMetadata;
    }

    public synchronized InstancesMetadata refreshInstancesMetadata()
    {
        // clean-up any open sessions or client resources
        close();
        setInstancesMetadata();
        return this.instancesMetadata;
    }

    public Session session()
    {
        return sessionProvider == null ? null : sessionProvider.get();
    }

    public void closeSessionProvider()
    {
        if (sessionProvider == null)
        {
            return;
        }

        sessionProvider.close();
    }

    @Override
    public String toString()
    {
        return "CassandraTestContext{" +
               ", version=" + version +
               ", cluster=" + abstractCassandraTestContext.cluster() +
               '}';
    }

    @Override
    public void close()
    {
        if (instancesMetadata != null)
        {
            instancesMetadata.instances().forEach(instance -> instance.delegate().close());
        }

        closeSessionProvider();
    }

    private void setInstancesMetadata()
    {
        this.instancesMetadata = buildInstancesMetadata(versionProvider, dnsResolver);
        for (InstancesMetadataListener listener : instancesMetadataListeners)
        {
            listener.onInstancesMetadataChange(this.instancesMetadata);
        }
    }

    public CQLSessionProviderImpl buildNewCqlSessionProvider()
    {
        UpgradeableCluster cluster = cluster();
        List<IInstanceConfig> configs = buildInstanceConfigs(cluster);
        List<InetSocketAddress> addresses = buildContactList(configs);
        return new CQLSessionProviderImpl(addresses, addresses, 500, null,
                                          0, username, password,
                                          sslConfiguration, SharedExecutorNettyOptions.INSTANCE);
    }

    private synchronized InstancesMetadata buildInstancesMetadata(CassandraVersionProvider versionProvider,
                                                                  DnsResolver dnsResolver)
    {
        UpgradeableCluster cluster = cluster();
        List<InstanceMetadata> metadata = new ArrayList<>();
        jmxClients = new ArrayList<>();
        List<IInstanceConfig> configs = buildInstanceConfigs(cluster);
        List<InetSocketAddress> addresses = buildContactList(configs);
        sessionProvider = new CQLSessionProviderImpl(addresses, addresses, 500, null,
                                                     0, username, password,
                                                     sslConfiguration, SharedExecutorNettyOptions.INSTANCE);
        for (int i = 0; i < configs.size(); i++)
        {
            if (configs.get(i) == null)
            {
                continue;
            }
            IInstanceConfig config = configs.get(i);
            String hostName = JMXUtil.getJmxHost(config);
            int nativeTransportPort = tryGetIntConfig(config, "native_transport_port", 9042);
            // The in-jvm dtest framework sometimes returns a cluster before all the jmx infrastructure is initialized.
            // In these cases, we want to wait longer than the default retry/delay settings to connect.
            JmxClient jmxClient = JmxClient.builder()
                                           .host(hostName)
                                           .port(config.jmxPort())
                                           .connectionMaxRetries(20)
                                           .connectionRetryDelay(SecondBoundConfiguration.ONE)
                                           .build();
            this.jmxClients.add(jmxClient);

            String[] dataDirectories = (String[]) config.get("data_file_directories");
            // Use the parent of the first data directory as the staging directory
            Path dataDirParentPath = Paths.get(dataDirectories[0]).getParent();
            // If the cluster has not started yet, the node's root directory doesn't exist yet
            assertThat(dataDirParentPath).isNotNull();
            Path stagingPath = dataDirParentPath.resolve("staging");
            String stagingDir = stagingPath.toFile().getAbsolutePath();

            MetricRegistry instanceSpecificRegistry = metricRegistryProvider.getOrCreate(i + 1);
            CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
                                                                             i + 1,
                                                                             versionProvider,
                                                                             sessionProvider,
                                                                             jmxClient,
                                                                             new DriverUtils(),
                                                                             "1.0-TEST",
                                                                             hostName,
                                                                             nativeTransportPort,
                                                                             new InstanceHealthMetrics(instanceSpecificRegistry));
            metadata.add(InstanceMetadataImpl.builder()
                                             .id(i + 1)
                                             .host(config.broadcastAddress().getAddress().getHostAddress())
                                             .port(nativeTransportPort)
                                             .dataDirs(Arrays.asList(dataDirectories))
                                             .cdcDir(config.getString("cdc_raw_directory"))
                                             .commitlogDir(config.getString("commitlog_directory"))
                                             .hintsDir(config.getString("hints_directory"))
                                             .savedCachesDir(config.getString("saved_caches_directory"))
                                             .stagingDir(stagingDir)
                                             .delegate(delegate)
                                             .metricRegistry(instanceSpecificRegistry)
                                             .build());
        }
        return new InstancesMetadataImpl(metadata, dnsResolver);
    }

    private static List<InetSocketAddress> buildContactList(List<IInstanceConfig> configs)
    {
        // Always return the complete list of addresses even if the cluster isn't yet that large
        // this way, we populate the entire local instance list
        return configs.stream()
                      .filter(Objects::nonNull)
                      .map(config -> new InetSocketAddress(config.broadcastAddress().getAddress(),
                                                           tryGetIntConfig(config, "native_transport_port", 9042)))
                      .collect(Collectors.toList());
    }

    @NotNull
    private List<IInstanceConfig> buildInstanceConfigs(UpgradeableCluster cluster)
    {
        Set<Integer> testManagedInstances;
        int maxNodeNum;
        if (instancesToManage == null)
        {
            testManagedInstances = null;
            maxNodeNum = cluster.size();
        }
        else
        {
            testManagedInstances = Arrays.stream(instancesToManage).boxed().collect(Collectors.toSet());
            // throws if test sets an empty array, it is a test configuration error
            maxNodeNum = Arrays.stream(instancesToManage).max().getAsInt();
        }
        return IntStream.range(1, maxNodeNum + 1)
                        .mapToObj(nodeNum -> {
                            // check whether the instances are managed by the test framework first. Because the nodeNum might be greater than the cluster size
                            if (manageInstanceByTestFramework() && cluster.get(nodeNum).isShutdown())
                            {
                                return null;
                            }

                            // Test supplies instances to manage. However, the set does not contain this nodeNum
                            if (testManagedInstances != null && !testManagedInstances.contains(nodeNum))
                            {
                                return null;
                            }

                            // The node should be managed by sidecar
                            return AbstractClusterUtils.createInstanceConfig(cluster, nodeNum);
                        })
                        .collect(Collectors.toList());
    }

    private boolean manageInstanceByTestFramework()
    {
        return instancesToManage == null;
    }

    /**
     * A listener for {@link InstancesMetadata} state changes
     */
    public interface InstancesMetadataListener
    {
        void onInstancesMetadataChange(InstancesMetadata instancesMetadata);
    }
}
