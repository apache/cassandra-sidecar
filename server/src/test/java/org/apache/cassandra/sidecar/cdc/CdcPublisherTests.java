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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.google.inject.Provider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.kafka.KafkaProducerFactory;
import org.apache.cassandra.cdc.kafka.TopicSupplier;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.ThrowingRunnable;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.CdcSystemViewsDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CACHE_WARMED_UP;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
    private CdcSystemViewsDatabaseAccessor systemViews;
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
        // Return a real successfully-completed Future so .onFailure(...) chaining in handle() is a safe no-op.
        when(taskExecutorPool.runBlocking(any(ThrowingRunnable.class))).thenReturn(Future.succeededFuture(null));

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
            systemViews,
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
        setupMocks("test-cdc-topic");

        EventConsumer result = cdcPublisher.eventConsumer(cdcConfig);

        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(CdcEventConsumer.class);
    }

    /**
     * Verifies that {@link CdcPublisher#eventConsumer(CdcConfig)} routes to the correct
     * {@link TopicSupplier} factory for every {@link CdcConfig.TopicFormatType} value.
     */
    @ParameterizedTest
    @EnumSource(CdcConfig.TopicFormatType.class)
    void testTopicSupplierRoutingPerTopicFormat(CdcConfig.TopicFormatType format)
    {
        String topicConfig = "cdc-%s-%s";
        setupMocks(topicConfig);
        when(cdcConfig.topicFormat()).thenReturn(format);

        Map<CdcConfig.TopicFormatType, MockedStatic.Verification> factories = new EnumMap<>(CdcConfig.TopicFormatType.class);
        factories.put(CdcConfig.TopicFormatType.STATIC,        () -> TopicSupplier.staticTopicSupplier(topicConfig));
        factories.put(CdcConfig.TopicFormatType.KEYSPACE,      () -> TopicSupplier.keyspaceSupplier(topicConfig));
        factories.put(CdcConfig.TopicFormatType.KEYSPACETABLE, () -> TopicSupplier.keyspaceTableSupplier(topicConfig));
        factories.put(CdcConfig.TopicFormatType.TABLE,         () -> TopicSupplier.tableSupplier(topicConfig));
        factories.put(CdcConfig.TopicFormatType.MAP,           () -> TopicSupplier.mapSupplier(topicConfig));

        try (MockedStatic<TopicSupplier> topicSupplier = Mockito.mockStatic(TopicSupplier.class, Mockito.RETURNS_MOCKS))
        {
            cdcPublisher.eventConsumer(cdcConfig);
            for (Map.Entry<CdcConfig.TopicFormatType, MockedStatic.Verification> entry : factories.entrySet())
            {
                topicSupplier.verify(entry.getValue(),
                                     entry.getKey() == format ? Mockito.times(1) : never());
            }
        }
    }

    private void setupMocks(String topicConfig)
    {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.put("bootstrap.servers", "localhost:9092");
        kafkaConfigs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfigs.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        when(cdcConfig.kafkaConfigs()).thenReturn(kafkaConfigs);
        when(cdcConfig.kafkaTopic()).thenReturn(topicConfig);
        when(cdcConfig.maxRecordSizeBytes()).thenReturn(1048576);
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
    }


    @Test
    void testHandleTokenRangeChangedDispatchesToWorkerPool() throws Exception
    {
        Message<Object> msg = mock(Message.class);
        when(msg.address()).thenReturn(RangeManager.RangeManagerEvents.ON_TOKEN_RANGE_CHANGED.address());

        CdcPublisher spyPublisher = spy(cdcPublisher);
        spyPublisher.handle(msg);

        ArgumentCaptor<ThrowingRunnable> captor = ArgumentCaptor.forClass(ThrowingRunnable.class);
        verify(taskExecutorPool).runBlocking(captor.capture());

        captor.getValue().run();
        verify(spyPublisher).handleTokenRangeChange();
    }

    @Test
    void testHandleTokenRangeGainedPassesEventToHandler() throws Exception
    {
        Message<Object> msg = mock(Message.class);
        when(msg.address()).thenReturn(RangeManager.LeadershipEvents.ON_TOKEN_RANGE_GAINED.address());
        RangeManager.RangeChangeEvent event = mock(RangeManager.RangeChangeEvent.class);
        when(msg.body()).thenReturn(event);

        CdcPublisher spyPublisher = spy(cdcPublisher);
        spyPublisher.handle(msg);

        ArgumentCaptor<ThrowingRunnable> captor = ArgumentCaptor.forClass(ThrowingRunnable.class);
        verify(taskExecutorPool).runBlocking(captor.capture());

        captor.getValue().run();
        verify(spyPublisher).handleRangeGained(event);
    }

    @Test
    void testHandleTokenRangeLostPassesEventToHandler() throws Exception
    {
        Message<Object> msg = mock(Message.class);
        when(msg.address()).thenReturn(RangeManager.LeadershipEvents.ON_TOKEN_RANGE_LOST.address());
        RangeManager.RangeChangeEvent event = mock(RangeManager.RangeChangeEvent.class);
        when(msg.body()).thenReturn(event);

        CdcPublisher spyPublisher = spy(cdcPublisher);
        spyPublisher.handle(msg);

        ArgumentCaptor<ThrowingRunnable> captor = ArgumentCaptor.forClass(ThrowingRunnable.class);
        verify(taskExecutorPool).runBlocking(captor.capture());

        captor.getValue().run();
        verify(spyPublisher).handleRangeLost(event);
    }

    @Test
    void testHandleServerStopRunsStopOnEventLoopWithoutWorkerPool()
    {
        Message<Object> msg = mock(Message.class);
        when(msg.address()).thenReturn(ON_SERVER_STOP.address());

        cdcPublisher = spy(cdcPublisher);
        cdcPublisher.handle(msg);

        // stop() must run synchronously on the event loop so shutdown completes
        // before the loop closes; no worker-pool dispatch should occur.
        verify(cdcPublisher).stop();
        verify(taskExecutorPool, never()).runBlocking(any(ThrowingRunnable.class));
    }

    @Test
    void testHandleCdcCacheWarmedUpRunsOnEventLoopWithoutWorkerPool()
    {
        Message<Object> msg = mock(Message.class);
        when(msg.address()).thenReturn(ON_CDC_CACHE_WARMED_UP.address());

        cdcPublisher.handle(msg);

        verify(taskExecutorPool, never()).runBlocking(any(ThrowingRunnable.class));
    }

    @Test
    void testHandleUnknownAddressIsNoOp()
    {
        Message<Object> msg = mock(Message.class);
        when(msg.address()).thenReturn("unknown.address");

        cdcPublisher.handle(msg);

        verify(taskExecutorPool, never()).runBlocking(any(ThrowingRunnable.class));
    }

    @Test
    void testHandleWorkerPoolFailureIsLoggedAndCaptured()
    {
        RuntimeException boom = new RuntimeException("boom");
        when(taskExecutorPool.runBlocking(any(ThrowingRunnable.class))).thenReturn(Future.failedFuture(boom));

        Message<Object> msg = mock(Message.class);
        when(msg.address()).thenReturn(RangeManager.RangeManagerEvents.ON_TOKEN_RANGE_CHANGED.address());

        cdcPublisher.handle(msg);

        // The discarded Future's failure path must reach our backstop:
        // captureUnrecoverableCdcError so the failure is observable in metrics.
        verify(sidecarCdcStats).captureUnrecoverableCdcError(boom);
    }
}
