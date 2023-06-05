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

package org.apache.cassandra.sidecar.common.testing;

import java.io.Closeable;
import java.net.InetSocketAddress;
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
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Passed to integration tests.
 * See {@link CassandraIntegrationTest} for the required annotation
 * See {@link CassandraTestTemplate} for the Test Template
 */
public class CassandraTestContext implements Closeable
{
    public final SimpleCassandraVersion version;
    public final UpgradeableCluster cluster;
    public final InstancesConfig instancesConfig;
    private List<CQLSessionProvider> sessionProviders;
    private List<JmxClient> jmxClients;

    CassandraTestContext(SimpleCassandraVersion version,
                         UpgradeableCluster cluster,
                         CassandraVersionProvider versionProvider)
    {
        this.version = version;
        this.cluster = cluster;
        this.sessionProviders = new ArrayList<>();
        this.jmxClients = new ArrayList<>();
        this.instancesConfig = buildInstancesConfig(versionProvider);
    }

    private InstancesConfig buildInstancesConfig(CassandraVersionProvider versionProvider)
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
            String uploadsStagingDirectory = dataDirParentPath.resolve("staging").toFile().getAbsolutePath();
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
        return new InstancesConfigImpl(metadata);
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
