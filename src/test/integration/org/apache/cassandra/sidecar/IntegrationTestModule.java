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

import java.util.Collections;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.TestValidationConfiguration;
import org.apache.cassandra.sidecar.common.testing.CassandraTestContext;
import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

/**
 * Provides the basic dependencies for integration tests
 */
public class IntegrationTestModule extends AbstractModule
{
    private final CassandraTestContext cassandraTestContext;

    public IntegrationTestModule(CassandraTestContext cassandraTestContext)
    {
        this.cassandraTestContext = cassandraTestContext;
    }

    @Provides
    @Singleton
    public InstancesConfig getInstancesConfig(CassandraVersionProvider versionProvider)
    {
        String dataDirectory = cassandraTestContext.dataDirectoryPath.toFile().getAbsolutePath();
        String uploadsStagingDirectory = cassandraTestContext.dataDirectoryPath.resolve("staging")
                                                                               .toFile().getAbsolutePath();
        InstanceMetadata metadata = new InstanceMetadataImpl(1,
                                                             "localhost",
                                                             9043,
                                                             Collections.singleton(dataDirectory),
                                                             uploadsStagingDirectory,
                                                             cassandraTestContext.session,
                                                             cassandraTestContext.jmxClient,
                                                             versionProvider);
        return new InstancesConfigImpl(metadata);
    }

    @Provides
    @Singleton
    public Configuration configuration(InstancesConfig instancesConfig,
                                       ValidationConfiguration validationConfiguration)
    {
        WorkerPoolConfiguration workPoolConf = new WorkerPoolConfiguration("test-pool", 10,
                                                                           30000);
        return new Configuration.Builder()
               .setInstancesConfig(instancesConfig)
               .setHost("localhost")
               .setPort(9043)
               .setRateLimitStreamRequestsPerSecond(1000L)
               .setValidationConfiguration(validationConfiguration)
               .setRequestIdleTimeoutMillis(300_000)
               .setRequestTimeoutMillis(300_000L)
               .setConcurrentUploadsLimit(80)
               .setMinSpacePercentRequiredForUploads(0)
               .setSSTableImportCacheConfiguration(new CacheConfiguration(60_000, 100))
               .setServerWorkerPoolConfiguration(workPoolConf)
               .setServerInternalWorkerPoolConfiguration(workPoolConf)
               .build();
    }

    @Provides
    @Singleton
    public ValidationConfiguration validationConfiguration()
    {
        return new TestValidationConfiguration();
    }
}
