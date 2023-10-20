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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.MockCassandraFactory;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.yaml.HealthCheckConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SSTableUploadConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ThrottleConfigurationImpl;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Provides the basic dependencies for unit tests.
 */
public class TestModule extends AbstractModule
{
    @Singleton
    @Provides
    public CassandraAdapterDelegate delegate()
    {
        return mock(CassandraAdapterDelegate.class);
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
        ThrottleConfiguration throttleConfiguration = new ThrottleConfigurationImpl(1, 10, 5);
        SSTableUploadConfiguration uploadConfiguration = new SSTableUploadConfigurationImpl(0F);
        ServiceConfiguration serviceConfiguration =
        ServiceConfigurationImpl.builder()
                                .host("127.0.0.1")
                                .port(0) // let the test find an available port
                                .throttleConfiguration(throttleConfiguration)
                                .ssTableUploadConfiguration(uploadConfiguration)
                                .build();
        HealthCheckConfiguration healthCheckConfiguration = new HealthCheckConfigurationImpl(200, 1000);
        return SidecarConfigurationImpl.builder()
                                       .serviceConfiguration(serviceConfiguration)
                                       .sslConfiguration(sslConfiguration)
                                       .healthCheckConfiguration(healthCheckConfiguration)
                                       .build();
    }

    @Provides
    @Singleton
    public InstancesConfig instancesConfig(DnsResolver dnsResolver)
    {
        return new InstancesConfigImpl(instancesMetas(), dnsResolver);
    }

    public List<InstanceMetadata> instancesMetas()
    {
        InstanceMetadata instance1 = mockInstance("localhost", 1, "src/test/resources/instance1/data", true);
        InstanceMetadata instance2 = mockInstance("localhost2", 2, "src/test/resources/instance2/data", false);
        InstanceMetadata instance3 = mockInstance("localhost3", 3, "src/test/resources/instance3/data", true);
        final List<InstanceMetadata> instanceMetas = new ArrayList<>();
        instanceMetas.add(instance1);
        instanceMetas.add(instance2);
        instanceMetas.add(instance3);
        return instanceMetas;
    }

    private InstanceMetadata mockInstance(String host, int id, String dataDir, boolean isUp)
    {
        InstanceMetadata instanceMeta = mock(InstanceMetadata.class);
        when(instanceMeta.id()).thenReturn(id);
        when(instanceMeta.host()).thenReturn(host);
        when(instanceMeta.port()).thenReturn(6475);
        when(instanceMeta.dataDirs()).thenReturn(Collections.singletonList(dataDir));

        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        if (isUp)
        {
            when(delegate.nodeSettings()).thenReturn(new NodeSettings(
            "testVersion", "testPartitioner", Collections.singletonMap("version", "testSidecar")));
        }
        when(delegate.isUp()).thenReturn(isUp);
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
