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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.MockCassandraFactory;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Provides the basic dependencies for unit tests.
 */
public class TestModule extends AbstractModule
{
    private static final Logger logger = LoggerFactory.getLogger(TestModule.class);

    @Singleton
    @Provides
    public CassandraAdapterDelegate delegate()
    {
        return mock(CassandraAdapterDelegate.class);
    }

    @Provides
    @Singleton
    public Configuration configuration()
    {
        return abstractConfig();
    }

    protected Configuration abstractConfig()
    {
        return new Configuration.Builder()
                           .setInstancesConfig(getInstancesConfig())
                           .setHost("127.0.0.1")
                           .setPort(6475)
                           .setHealthCheckFrequency(1000)
                           .setSslEnabled(false)
                           .setRateLimitStreamRequestsPerSecond(1)
                           .setThrottleDelayInSeconds(5)
                           .setThrottleTimeoutInSeconds(10)
                           .build();
    }

    @Provides
    @Singleton
    public InstancesConfig getInstancesConfig()
    {
        return new InstancesConfigImpl(getInstanceMetas());
    }

    public List<InstanceMetadata> getInstanceMetas()
    {
        InstanceMetadata instance1 = getMockInstance("localhost", 1, "src/test/resources/instance1/data", true);
        InstanceMetadata instance2 = getMockInstance("localhost2", 2, "src/test/resources/instance2/data", false);
        InstanceMetadata instance3 = getMockInstance("localhost3", 3, "src/test/resources/instance3/data", true);
        final List<InstanceMetadata> instanceMetas = new ArrayList<>();
        instanceMetas.add(instance1);
        instanceMetas.add(instance2);
        instanceMetas.add(instance3);
        return instanceMetas;
    }

    private InstanceMetadata getMockInstance(String host, int id, String dataDir, boolean isUp)
    {
        InstanceMetadata instanceMeta = mock(InstanceMetadata.class);
        when(instanceMeta.id()).thenReturn(id);
        when(instanceMeta.host()).thenReturn(host);
        when(instanceMeta.port()).thenReturn(6475);
        when(instanceMeta.dataDirs()).thenReturn(Collections.singletonList(dataDir));

        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        when(delegate.isUp()).thenReturn(isUp);
        doNothing().when(delegate).start();
        when(instanceMeta.delegate()).thenReturn(delegate);
        return instanceMeta;
    }

    @Provides
    @Singleton
    public FileSystem fileSystem(Vertx vertx)
    {
        return vertx.fileSystem();
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
