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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SSTableUploadConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ThrottleConfigurationImpl;
import org.apache.cassandra.sidecar.testing.CassandraSidecarTestContext;

/**
 * Provides the basic dependencies for integration tests
 */
public class IntegrationTestModule extends AbstractModule
{
    private final CassandraSidecarTestContext cassandraTestContext;

    public IntegrationTestModule(CassandraSidecarTestContext cassandraTestContext)
    {
        this.cassandraTestContext = cassandraTestContext;
    }

    @Provides
    @Singleton
    public InstancesConfig instancesConfig()
    {
        return cassandraTestContext.getInstancesConfig();
    }

    @Provides
    @Singleton
    public SidecarConfiguration configuration()
    {
        ThrottleConfiguration throttleConfiguration = new ThrottleConfigurationImpl(1000L);
        SSTableUploadConfiguration ssTableUploadConfiguration = new SSTableUploadConfigurationImpl(0F);
        ServiceConfiguration serviceConfiguration = new ServiceConfigurationImpl("127.0.0.1",
                                                                                 throttleConfiguration,
                                                                                 ssTableUploadConfiguration);
        return new SidecarConfigurationImpl(serviceConfiguration);
    }
}
