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

import com.google.inject.Provider;
import io.vertx.core.Vertx;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.sidecar.cdc.CdcConfig;
import org.apache.cassandra.sidecar.cdc.CdcPublisher;
import org.apache.cassandra.sidecar.cdc.SidecarCdcStats;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.VirtualTablesDatabaseAccessor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Test implementation of CdcPublisher that uses an in-memory event consumer
 * instead of publishing to Kafka. This allows integration tests to capture
 * and verify CDC events without requiring a Kafka infrastructure.
 */
public class TestCdcPublisher extends CdcPublisher
{
    private final TestCdcEventConsumer testEventConsumer = new TestCdcEventConsumer();
    private final CdcDatabaseAccessor databaseAccessor;

    public TestCdcPublisher(Vertx vertx,
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
                           Provider<RangeManager> rangeManagerProvider)
    {
        super(vertx, sidecarConfiguration, executorPools, clusterConfigProvider,
              schemaSupplier, sidecarInstancesProvider, clientConfig,
              instanceMetadataFetcher, conf, databaseAccessor, cdcStats,
              virtualTables, sidecarCdcStats, avroSerializer, rangeManagerProvider);
        this.databaseAccessor = databaseAccessor;
    }

    @Override
    public EventConsumer eventConsumer(CdcConfig conf, Serializer<CdcEvent> avroSerializer)
    {
        return testEventConsumer;
    }

    /**
     * Override scheduleDecision to execute in tests when database is ready,
     * bypassing the initialization and cache warming checks required in production.
     */
    @Override
    public ScheduleDecision scheduleDecision()
    {
        // If already running, skip to avoid redundant execution attempts
        if (isRunning())
        {
            return ScheduleDecision.SKIP;
        }

        // Only execute if the database accessor is available
        // This prevents CassandraUnavailableException during test startup
        if (databaseAccessor.isAvailable())
        {
            return ScheduleDecision.EXECUTE;
        }

        // Database not ready yet, skip this iteration and retry later
        return ScheduleDecision.SKIP;
    }

    /**
     * @return the test CDC event consumer for test assertions
     */
    public TestCdcEventConsumer getTestEventConsumer()
    {
        return testEventConsumer;
    }
}
