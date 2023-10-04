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

package org.apache.cassandra.sidecar.test;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.Session;
import io.vertx.core.Vertx;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.utils.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.AbstractCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Passed to integration tests.
 */
public class CassandraSidecarTestContext implements AutoCloseable
{
    public final SimpleCassandraVersion version;
    private final CassandraVersionProvider versionProvider;
    private final DnsResolver dnsResolver;
    private final AbstractCassandraTestContext abstractCassandraTestContext;
    private final Vertx vertx;
    public InstancesConfig instancesConfig;
    private List<CQLSessionProvider> sessionProviders;
    private List<JmxClient> jmxClients;
    private static final SidecarVersionProvider svp = new SidecarVersionProvider("/sidecar.version");
    private final List<InstanceConfigListener> instanceConfigListeners;

    private CassandraSidecarTestContext(Vertx vertx,
                                        AbstractCassandraTestContext abstractCassandraTestContext,
                                        SimpleCassandraVersion version,
                                        CassandraVersionProvider versionProvider,
                                        DnsResolver dnsResolver)
    {
        this.vertx = vertx;
        this.instanceConfigListeners = new ArrayList<>();
        this.abstractCassandraTestContext = abstractCassandraTestContext;
        this.version = version;
        this.versionProvider = versionProvider;
        this.dnsResolver = dnsResolver;
    }

    public static CassandraSidecarTestContext from(Vertx vertx,
                                                   AbstractCassandraTestContext cassandraTestContext,
                                                   DnsResolver dnsResolver)
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
                                               dnsResolver);
    }

    public static CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver)
    {
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, svp.sidecarVersion())).build();
    }

    public void registerInstanceConfigListener(InstanceConfigListener listener)
    {
        this.instanceConfigListeners.add(listener);
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

    public InstancesConfig instancesConfig()
    {
        if (instancesConfig == null
            || instancesConfig.instances().size() != cluster().size()) // rebuild instances config if cluster changed
        {
            // clean-up any open sessions or client resources
            close();
            setInstancesConfig();
        }
        return this.instancesConfig;
    }

    public Session session()
    {
        return session(0);
    }

    public Session session(int instance)
    {
        if (sessionProviders == null)
        {
            setInstancesConfig();
        }
        return this.sessionProviders.get(instance).localCql();
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
        if (sessionProviders != null)
        {
            sessionProviders.forEach(CQLSessionProvider::close);
        }
        if (instancesConfig != null)
        {
            instancesConfig.instances().forEach(instance -> instance.delegate().close());
        }
    }

    public JmxClient jmxClient()
    {
        return jmxClient(0);
    }

    private JmxClient jmxClient(int instance)
    {
        if (jmxClients == null)
        {
            setInstancesConfig();
        }
        return jmxClients.get(instance);
    }

    /**
     * A listener for {@link InstancesConfig} state changes
     */
    public interface InstanceConfigListener
    {
        void onInstancesConfigChange(InstancesConfig instancesConfig);
    }

    private void setInstancesConfig()
    {
        this.instancesConfig = buildInstancesConfig(versionProvider, dnsResolver);
        for (InstanceConfigListener listener : instanceConfigListeners)
        {
            listener.onInstancesConfigChange(this.instancesConfig);
        }
    }

    private InstancesConfig buildInstancesConfig(CassandraVersionProvider versionProvider,
                                                 DnsResolver dnsResolver)
    {
        UpgradeableCluster cluster = cluster();
        List<InstanceMetadata> metadata = new ArrayList<>();
        sessionProviders = new ArrayList<>();
        jmxClients = new ArrayList<>();
        for (int i = 0; i < cluster.size(); i++)
        {
            IUpgradeableInstance instance = cluster.get(i + 1); // 1-based indexing to match node names;
            IInstanceConfig config = instance.config();
            String hostName = JMXUtil.getJmxHost(config);
            int nativeTransportPort = tryGetIntConfig(config, "native_transport_port", 9042);
            InetSocketAddress address = InetSocketAddress.createUnresolved(hostName,
                                                                           nativeTransportPort);
            CQLSessionProvider sessionProvider = new CQLSessionProvider(address, new NettyOptions());
            this.sessionProviders.add(sessionProvider);
            // The in-jvm dtest framework sometimes returns a cluster before all the jmx infrastructure is initialized.
            // In these cases, we want to wait longer than the default retry/delay settings to connect.
            JmxClient jmxClient = JmxClient.builder()
                                           .host(hostName)
                                           .port(config.jmxPort())
                                           .connectionMaxRetries(20)
                                           .connectionRetryDelayMillis(1000L)
                                           .build();
            this.jmxClients.add(jmxClient);

            String[] dataDirectories = (String[]) config.get("data_file_directories");
            // Use the parent of the first data directory as the staging directory
            Path dataDirParentPath = Paths.get(dataDirectories[0]).getParent();
            // If the cluster has not started yet, the node's root directory doesn't exist yet
            assertThat(dataDirParentPath).isNotNull();
            Path stagingPath = dataDirParentPath.resolve("staging");
            String stagingDir = stagingPath.toFile().getAbsolutePath();
            CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
                                                                             i + 1,
                                                                             versionProvider,
                                                                             sessionProvider,
                                                                             jmxClient,
                                                                             "1.0-TEST");
            metadata.add(InstanceMetadataImpl.builder()
                                             .id(i + 1)
                                             .host(config.broadcastAddress().getAddress().getHostAddress())
                                             .port(nativeTransportPort)
                                             .dataDirs(Arrays.asList(dataDirectories))
                                             .stagingDir(stagingDir)
                                             .delegate(delegate)
                                             .build());
        }
        return new InstancesConfigImpl(metadata, dnsResolver);
    }

    private static int tryGetIntConfig(IInstanceConfig config, String configName, int defaultValue)
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
}
