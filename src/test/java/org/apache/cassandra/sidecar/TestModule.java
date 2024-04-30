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

package org.apache.cassandra.sidecar;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SidecarRateLimiter;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.MockCassandraFactory;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.yaml.HealthCheckConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.RestoreJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SSTableUploadConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SchemaKeyspaceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ThrottleConfigurationImpl;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;

import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Provides the basic dependencies for unit tests.
 */
public class TestModule extends AbstractModule
{
    public static final int RESTORE_MAX_CONCURRENCY = 10;

    public TestCassandraAdapterDelegate delegate;

    public TestModule()
    {
        this(new TestCassandraAdapterDelegate());
    }

    public TestModule(TestCassandraAdapterDelegate delegate)
    {
        this.delegate = delegate;
    }

    @Singleton
    @Provides
    public CassandraAdapterDelegate delegate()
    {
        return delegate;
    }

    @Provides
    @Singleton
    public SidecarConfiguration configuration()
    {
        return abstractConfig();
    }

    protected SidecarConfigurationImpl abstractConfig()
    {
        return abstractConfig(null);
    }

    protected SidecarConfigurationImpl abstractConfig(SslConfiguration sslConfiguration)
    {
        ThrottleConfiguration throttleConfiguration = new ThrottleConfigurationImpl(5, 5, 1);
        SSTableUploadConfiguration uploadConfiguration = new SSTableUploadConfigurationImpl(0F);
        SchemaKeyspaceConfiguration schemaKeyspaceConfiguration =
        SchemaKeyspaceConfigurationImpl.builder()
                                       .isEnabled(true)
                                       .keyspace("sidecar_internal")
                                       .replicationFactor(1)
                                       .replicationStrategy("SimpleStrategy")
                                       .build();
        ServiceConfiguration serviceConfiguration =
        TestServiceConfiguration.builder()
                                .throttleConfiguration(throttleConfiguration)
                                .schemaKeyspaceConfiguration(schemaKeyspaceConfiguration)
                                .sstableUploadConfiguration(uploadConfiguration)
                                .build();
        RestoreJobConfiguration restoreJobConfiguration =
        RestoreJobConfigurationImpl.builder()
                                   .restoreJobTablesTtlSeconds(TimeUnit.DAYS.toSeconds(14) + 1)
                                   .processMaxConcurrency(RESTORE_MAX_CONCURRENCY)
                                   .restoreJobLongRunningThresholdSeconds(1)
                                   .build();
        HealthCheckConfiguration healthCheckConfiguration = new HealthCheckConfigurationImpl(200, 1000);
        return SidecarConfigurationImpl.builder()
                                       .serviceConfiguration(serviceConfiguration)
                                       .sslConfiguration(sslConfiguration)
                                       .restoreJobConfiguration(restoreJobConfiguration)
                                       .healthCheckConfiguration(healthCheckConfiguration)
                                       .build();
    }

    @Provides
    @Singleton
    public InstancesConfig instancesConfig(DnsResolver dnsResolver)
    {
        return new InstancesConfigImpl(instancesMetas(), dnsResolver);
    }

    @Provides
    @Singleton
    @Named("IngressFileRateLimiter")
    public SidecarRateLimiter ingressFileRateLimiter(SidecarConfiguration sidecarConfiguration)
    {
        return SidecarRateLimiter.create(sidecarConfiguration.serviceConfiguration()
                                                             .trafficShapingConfiguration()
                                                             .inboundGlobalFileBandwidthBytesPerSecond());
    }

    public List<InstanceMetadata> instancesMetas()
    {
        InstanceMetadata instance1 = mockInstance("localhost",
                                                  1,
                                                  "src/test/resources/instance1/data",
                                                  "src/test/resources/instance1/sstable-staging",
                                                  true);
        InstanceMetadata instance2 = mockInstance("localhost2",
                                                  2,
                                                  "src/test/resources/instance2/data",
                                                  "src/test/resources/instance2/sstable-staging",
                                                  false);
        InstanceMetadata instance3 = mockInstance("localhost3",
                                                  3,
                                                  "src/test/resources/instance3/data",
                                                  "src/test/resources/instance3/sstable-staging",
                                                  true);
        final List<InstanceMetadata> instanceMetas = new ArrayList<>();
        instanceMetas.add(instance1);
        instanceMetas.add(instance2);
        instanceMetas.add(instance3);
        return instanceMetas;
    }

    private InstanceMetadata mockInstance(String host, int id, String dataDir, String stagingDir, boolean isUp)
    {
        InstanceMetadata instanceMeta = mock(InstanceMetadata.class);
        when(instanceMeta.id()).thenReturn(id);
        when(instanceMeta.host()).thenReturn(host);
        when(instanceMeta.port()).thenReturn(6475);
        when(instanceMeta.stagingDir()).thenReturn(stagingDir);
        List<String> dataDirectories = Collections.singletonList(dataDir);
        when(instanceMeta.dataDirs()).thenReturn(dataDirectories);
        when(instanceMeta.metrics()).thenReturn(new InstanceMetricsImpl(registry(id)));

        StorageOperations mockStorageOperations = mock(StorageOperations.class);
        when(mockStorageOperations.dataFileLocations()).thenReturn(dataDirectories);
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(metadata.getKeyspace(any())).thenReturn(keyspaceMetadata);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(keyspaceMetadata.getTable(any())).thenReturn(tableMetadata);
        delegate.setMetadata(metadata);
        delegate.setStorageOperations(mockStorageOperations);
        if (isUp)
        {
            delegate.setNodeSettings(NodeSettings.builder()
                                                 .releaseVersion("testVersion")
                                                 .partitioner("testPartitioner")
                                                 .sidecarVersion("testSidecar")
                                                 .datacenter("testDC")
                                                 .rpcAddress(InetAddress.getLoopbackAddress())
                                                 .rpcPort(6475)
                                                 .tokens(Collections.singleton("testToken"))
                                                 .build());
        }
        delegate.setIsNativeUp(isUp);
        when(instanceMeta.delegate()).thenReturn(delegate);
        return instanceMeta;
    }

    /**
     * The Mock factory is used for testing purposes, enabling us to test all failures and possible results
     *
     * @return the {@link CassandraVersionProvider}
     */
    @Provides
    @Singleton
    public CassandraVersionProvider cassandraVersionProvider()
    {
        CassandraVersionProvider.Builder builder = new CassandraVersionProvider.Builder();
        builder.add(new MockCassandraFactory());
        return builder.build();
    }
}
