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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.Session;
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
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Passed to integration tests.
 */
public class CassandraSidecarTestContext extends CassandraTestContext
{
    public final SimpleCassandraVersion version;
    public final UpgradeableCluster cluster;
    public final InstancesConfig instancesConfig;
    private List<CQLSessionProvider> sessionProviders;
    private List<JmxClient> jmxClients;
    private static final SidecarVersionProvider svp = new SidecarVersionProvider("/sidecar.version");

    private CassandraSidecarTestContext(SimpleCassandraVersion version,
                                        UpgradeableCluster cluster,
                                        CassandraVersionProvider versionProvider, DnsResolver dnsResolver) throws IOException
    {
        super(org.apache.cassandra.testing.SimpleCassandraVersion.create(version.major,
                                                                         version.minor,
                                                                         version.patch), cluster);
        this.version = version;
        this.cluster = cluster;
        this.sessionProviders = new ArrayList<>();
        this.jmxClients = new ArrayList<>();
        this.instancesConfig = buildInstancesConfig(versionProvider, dnsResolver);
    }

    public static CassandraSidecarTestContext from(CassandraTestContext cassandraTestContext, DnsResolver dnsResolver)
    {
        org.apache.cassandra.testing.SimpleCassandraVersion rootVersion = cassandraTestContext.version;
        SimpleCassandraVersion versionParsed = SimpleCassandraVersion.create(rootVersion.major,
                                                                             rootVersion.minor,
                                                                             rootVersion.patch);
        CassandraVersionProvider versionProvider = cassandraVersionProvider(dnsResolver);
        try
        {
            return new CassandraSidecarTestContext(versionParsed, cassandraTestContext.getCluster(), versionProvider, dnsResolver);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver)
    {
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, svp.sidecarVersion())).build();
    }

    private InstancesConfig buildInstancesConfig(CassandraVersionProvider versionProvider, DnsResolver dnsResolver) throws IOException
    {
        List<InstanceMetadata> metadata = new ArrayList<>();
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
            JmxClient jmxClient = new JmxClient(hostName, config.jmxPort());
            this.jmxClients.add(jmxClient);

            String[] dataDirectories = (String[]) config.get("data_file_directories");
            // Use the parent of the first data directory as the staging directory
            Path dataDirParentPath = Paths.get(dataDirectories[0]).getParent();
            assertThat(dataDirParentPath).isNotNull()
                      .exists();
            Path stagingPath = dataDirParentPath.resolve("staging");
            String uploadsStagingDirectory = stagingPath.toFile().getAbsolutePath();
            if (!Files.exists(stagingPath))
            {
                Files.createDirectory(stagingPath);
            }
            metadata.add(new InstanceMetadataImpl(i + 1,
                                                  config.broadcastAddress().getAddress().getHostAddress(),
                                                  nativeTransportPort,
                                                  Arrays.asList(dataDirectories),
                                                  uploadsStagingDirectory,
                                                  sessionProvider,
                                                  jmxClient,
                                                  versionProvider,
                                                  "1.0-TEST"));
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


    public InstancesConfig getInstancesConfig()
    {
        return this.instancesConfig;
    }

    public Session session()
    {
        return session(0);
    }

    public Session session(int instance)
    {
        return this.sessionProviders.get(instance).localCql();
    }

    @Override
    public String toString()
    {
        return "CassandraTestContext{" +
               ", version=" + version +
               ", cluster=" + cluster +
               '}';
    }
    @Override
    public void close()
    {
        instancesConfig.instances().forEach(instance -> instance.delegate().close());
        // TODO: Do we need to close sessions?
//        sessionProviders.
    }

    public JmxClient jmxClient()
    {
        return jmxClient(0);
    }

    private JmxClient jmxClient(int instance)
    {
        return jmxClients.get(instance);
    }
}
