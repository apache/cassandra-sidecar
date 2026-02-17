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

import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.cdc.CdcConfig;
import org.apache.cassandra.sidecar.cdc.CdcPublisher;
import org.apache.cassandra.sidecar.cdc.SidecarCdcStats;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ContentionFreeRangeManager;
import org.apache.cassandra.sidecar.coordination.TokenRingProvider;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.VirtualTablesDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.kafka.common.serialization.Serializer;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Base class for CDC integration tests. Extends SharedClusterIntegrationTestBase with
 * CDC-specific configuration and setup, including:
 * - CDC-enabled Cassandra cluster configuration
 * - TestCdcPublisher with TestCdcEventConsumer
 * - Cassandra 4.1 version requirement
 * - Helper methods to access CDC components
 */
public abstract class SharedClusterCdcSidecarIntegrationTestBase extends SharedClusterIntegrationTestBase
{
    @AfterEach
    void cleanupCdcConsumerAfterEachTest()
    {
        TestCdcPublisher testCdcPublisher = (TestCdcPublisher) serverWrapper.injector.getInstance(CdcPublisher.class);
        if (testCdcPublisher != null)
        {
            TestCdcEventConsumer consumer = testCdcPublisher.getTestEventConsumer();
            if (consumer != null)
            {
                consumer.clear();
            }
        }
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        // The current CDC implementation cannot read 5.x commitlogs, so verify Cassandra version is 4.x
        SimpleCassandraVersion version = SimpleCassandraVersion.create(testVersion.version());
        assumeThat(version.major)
                .as("Current CDC implementation cannot read 5.x commitlogs, requires Cassandra 4.x")
                .isEqualTo(4);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                .dcCount(1)
                .nodesPerDc(1)
                .additionalInstanceConfig(Map.of("cdc_enabled", true));
    }

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> {
            // Override service configuration to use specific port for CDC tests
            ServiceConfiguration existingConfig = builder.build().serviceConfiguration();
            ServiceConfiguration cdcServiceConfig = ServiceConfigurationImpl.builder()
                                                                            .host(existingConfig.host())
                                                                            .port(9043)  // TODO: Make this port dynamically allocated
                                                                            .schemaKeyspaceConfiguration(existingConfig.schemaKeyspaceConfiguration())
                                                                            .build();
            builder.serviceConfiguration(cdcServiceConfig);

            // Configure sidecar client for mTLS if enabled
            SidecarClientConfiguration clientConfig = mtlsTestHelper.createSidecarClientConfiguration();
            if (clientConfig != null)
            {
                builder.sidecarClientConfiguration(clientConfig);
            }
            return builder;
        };
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        AbstractModule cdcModule = new CdcTestModule();
        serverWrapper = startSidecarWithInstances(cluster, cdcModule);
    }

    /**
     * @return the TestCdcPublisher instance for test access
     */
    protected TestCdcPublisher getCdcPublisher()
    {
        return (TestCdcPublisher) serverWrapper.injector.getInstance(CdcPublisher.class);
    }

    /**
     * @return the TestCdcEventConsumer for test assertions
     */
    protected TestCdcEventConsumer getTestEventConsumer()
    {
        TestCdcPublisher publisher = getCdcPublisher();
        return publisher != null ? publisher.getTestEventConsumer() : null;
    }

    /**
     * CDC-specific Guice module that provides test implementations for CDC components.
     */
    private static class CdcTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        CdcPublisher cdcPublisher(Vertx vertx,
                                  SidecarConfiguration sidecarConfiguration,
                                  ExecutorPools executorPools,
                                  ClusterConfigProvider clusterConfigProvider,
                                  SchemaSupplier schemaSupplier,
                                  CdcSidecarInstancesProvider sidecarInstancesProvider,
                                  SidecarCdcClient.ClientConfig clientConfig,
                                  InstanceMetadataFetcher instanceMetadataFetcher,
                                  CdcConfig conf,
                                  CdcDatabaseAccessor databaseAccessor,
                                  ICdcStats cdcStats,
                                  VirtualTablesDatabaseAccessor virtualTables,
                                  SidecarCdcStats sidecarCdcStats,
                                  Serializer<CdcEvent> avroSerializer,
                                  TokenRingProvider tokenRingProvider)
        {
            return new TestCdcPublisher(vertx,
                                       sidecarConfiguration,
                                       executorPools,
                                       clusterConfigProvider,
                                       schemaSupplier,
                                       sidecarInstancesProvider,
                                       clientConfig,
                                       instanceMetadataFetcher,
                                       conf,
                                       databaseAccessor,
                                       cdcStats,
                                       virtualTables,
                                       sidecarCdcStats,
                                       avroSerializer,
                                       () -> new ContentionFreeRangeManager(vertx, tokenRingProvider));
        }

        @Provides
        @Singleton
        public CdcConfig cdcConfig()
        {
            return new TestCdcConfig();
        }
    }
}
