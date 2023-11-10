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

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.HealthCheckConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.jetbrains.annotations.NotNull;

/**
 * Provides the basic dependencies for integration tests
 */
public class IntegrationTestModule extends AbstractModule
{
    private CassandraSidecarTestContext cassandraTestContext;

    public void setCassandraTestContext(CassandraSidecarTestContext cassandraTestContext)
    {
        this.cassandraTestContext = cassandraTestContext;
    }

    @Provides
    @Singleton
    public InstancesConfig instancesConfig()
    {
        return new WrapperInstancesConfig();
    }

    @Provides
    @Singleton
    public SidecarConfiguration configuration()
    {
        ServiceConfiguration conf = ServiceConfigurationImpl.builder()
                                                            .host("127.0.0.1")
                                                            .port(0) // let the test find an available port
                                                            .build();
        HealthCheckConfiguration healthCheckConfiguration = new HealthCheckConfigurationImpl(50, 500);
        return SidecarConfigurationImpl.builder()
                                       .serviceConfiguration(conf)
                                       .healthCheckConfiguration(healthCheckConfiguration)
                                       .build();
    }

    class WrapperInstancesConfig implements InstancesConfig
    {
        /**
         * @return metadata of instances owned by the sidecar
         */
        @Override
        @NotNull
        public List<InstanceMetadata> instances()
        {
            if (cassandraTestContext != null && cassandraTestContext.isClusterBuilt())
                return cassandraTestContext.instancesConfig().instances();
            return Collections.emptyList();
        }

        /**
         * Lookup instance metadata by id.
         *
         * @param id instance's id
         * @return instance meta information
         * @throws NoSuchElementException when the instance with {@code id} does not exist
         */
        public InstanceMetadata instanceFromId(int id) throws NoSuchElementException
        {
            return cassandraTestContext.instancesConfig().instanceFromId(id);
        }

        /**
         * Lookup instance metadata by host name.
         *
         * @param host host address of instance
         * @return instance meta information
         * @throws NoSuchElementException when the instance for {@code host} does not exist
         */
        public InstanceMetadata instanceFromHost(String host) throws NoSuchElementException
        {
            return cassandraTestContext.instancesConfig().instanceFromHost(host);
        }
    }
}
