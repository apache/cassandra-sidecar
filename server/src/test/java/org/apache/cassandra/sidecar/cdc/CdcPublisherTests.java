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

package org.apache.cassandra.sidecar.cdc;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.inject.Provider;
import io.vertx.core.Vertx;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.kafka.KafkaProducerFactory;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.VirtualTablesDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CdcPublisher
 */
public class CdcPublisherTests
{
    @Mock
    private Vertx vertx;
    @Mock
    private ExecutorPools executorPools;
    @Mock
    private TaskExecutorPool taskExecutorPool;
    @Mock
    private ClusterConfigProvider clusterConfigProvider;
    @Mock
    private SchemaSupplier schemaSupplier;
    @Mock
    private InstanceMetadataFetcher instanceMetadataFetcher;
    @Mock
    private CdcDatabaseAccessor databaseAccessor;
    @Mock
    private ICdcStats cdcStats;
    @Mock
    private VirtualTablesDatabaseAccessor virtualTables;
    @Mock
    private SidecarCdcStats sidecarCdcStats;
    @Mock
    private KafkaProducerFactory kafkaProducerFactory;
    @Mock
    private CachingSchemaStore schemaStore;
    @Mock
    private Provider<RangeManager> rangeManager;
    @Mock
    private CassandraBridgeFactory cassandraBridgeFactory;
    @Mock
    private SidecarCdcClient sidecarCdcClient;
    @Mock
    private CdcOptions cdcOptions;

    private CdcConfig cdcConfig;
    private CdcPublisher cdcPublisher;

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.openMocks(this);

        cdcConfig = mock(CdcConfig.class, RETURNS_DEEP_STUBS);

        // Mock ExecutorPools behavior
        when(executorPools.internal()).thenReturn(taskExecutorPool);

        // Mock Vertx EventBus for event listeners
        when(vertx.eventBus()).thenReturn(mock(io.vertx.core.eventbus.EventBus.class, RETURNS_DEEP_STUBS));

        cdcPublisher = new CdcPublisher(
            vertx,
            executorPools,
            clusterConfigProvider,
            schemaSupplier,
            instanceMetadataFetcher,
            cdcConfig,
            databaseAccessor,
            cdcStats,
            virtualTables,
            sidecarCdcStats,
            rangeManager,
            cassandraBridgeFactory,
            () -> sidecarCdcClient,
            schemaStore,
            kafkaProducerFactory,
            cdcOptions
        );
    }

    @Test
    void testEventConsumerCreatesValidConsumer()
    {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.put("bootstrap.servers", "localhost:9092");
        kafkaConfigs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfigs.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        when(cdcConfig.kafkaConfigs()).thenReturn(kafkaConfigs);
        when(cdcConfig.kafkaTopic()).thenReturn("test-cdc-topic");
        when(cdcConfig.maxRecordSizeBytes()).thenReturn(1048576); // 1MB
        when(cdcConfig.failOnRecordTooLargeError()).thenReturn(false);
        when(cdcConfig.failOnKafkaError()).thenReturn(true);

        InstanceMetadata mockInstance = mock(InstanceMetadata.class, RETURNS_DEEP_STUBS);
        when(mockInstance.delegate().nodeSettings().releaseVersion()).thenReturn("4.1.0");
        doAnswer(invocation -> {
            Function<InstanceMetadata, Object> fn = invocation.getArgument(0);
            return fn.apply(mockInstance);
        }).when(instanceMetadataFetcher).callOnFirstAvailableInstance(any());
        CassandraBridge mockBridge = mock(CassandraBridge.class);
        when(mockBridge.getVersion()).thenReturn(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(mockBridge);
        when(kafkaProducerFactory.create(any())).thenReturn(mock(KafkaProducer.class));

        EventConsumer result = cdcPublisher.eventConsumer(cdcConfig);

        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(CdcEventConsumer.class);
    }
}
