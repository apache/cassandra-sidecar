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

package org.apache.cassandra.sidecar.tasks;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Promise;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.DriverUnsupportedSchemaCache;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.apache.cassandra.sidecar.config.yaml.CdcConfigurationImpl.DEFAULT_TABLE_SCHEMA_REFRESH_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CassandraClusterSchemaMonitor}
 */
class CassandraClusterSchemaMonitorTest
{
    private CassandraClusterSchemaMonitor clusterSchema;
    private InstanceMetadataFetcher mockInstanceFetcher;
    private CdcDatabaseAccessor mockDatabaseAccessor;
    private DriverUnsupportedSchemaCache mockDriverUnsupportedSchemaCache;
    private SidecarConfiguration mockSidecarConfiguration;
    private ServiceConfiguration mockServiceConfiguration;
    private CdcConfiguration mockCdcConfiguration;
    private SchemaKeyspaceConfiguration mockSchemaKeyspaceConfiguration;
    private NodeSettings mockNodeSettings;
    private CassandraBridge mockCassandraBridge;
    private CdcBridge mockCdcBridge;
    private CassandraBridgeFactory mockCassandraBridgeFactory;

    private static final String INITIAL_SCHEMA = "CREATE TABLE test.cdc_table (\n" +
                                                 "    id uuid PRIMARY KEY,\n" +
                                                 "    data text\n" +
                                                 ") WITH cdc = true;";

    private static final String UPDATED_SCHEMA = "CREATE TABLE test.cdc_table (\n" +
                                                 "    id uuid PRIMARY KEY,\n" +
                                                 "    data text,\n" +
                                                 "    timestamp timestamp\n" +
                                                 ") WITH cdc = true;\n" +
                                                 "CREATE TABLE test.another_cdc_table (\n" +
                                                 "    id uuid PRIMARY KEY,\n" +
                                                 "    value int\n" +
                                                 ") WITH cdc = true;";

    @BeforeEach
    void setup()
    {
        mockInstanceFetcher = mock(InstanceMetadataFetcher.class);
        mockDatabaseAccessor = mock(CdcDatabaseAccessor.class);
        mockDriverUnsupportedSchemaCache = mock(DriverUnsupportedSchemaCache.class);
        mockSidecarConfiguration = mock(SidecarConfiguration.class);
        mockServiceConfiguration = mock(ServiceConfiguration.class);
        mockCdcConfiguration = mock(CdcConfiguration.class);
        mockSchemaKeyspaceConfiguration = mock(SchemaKeyspaceConfiguration.class);
        mockNodeSettings = mock(NodeSettings.class);
        mockCassandraBridge = mock(CassandraBridge.class);
        mockCdcBridge = mock(CdcBridge.class);
        mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);

        // Setup configuration chain
        when(mockSidecarConfiguration.serviceConfiguration()).thenReturn(mockServiceConfiguration);
        when(mockServiceConfiguration.cdcConfiguration()).thenReturn(mockCdcConfiguration);
        when(mockServiceConfiguration.schemaKeyspaceConfiguration()).thenReturn(mockSchemaKeyspaceConfiguration);

        // Setup default enabled configurations
        when(mockCdcConfiguration.isEnabled()).thenReturn(true);
        when(mockCdcConfiguration.tableSchemaRefreshTime()).thenReturn(SecondBoundConfiguration.parse("60s"));
        when(mockSchemaKeyspaceConfiguration.isEnabled()).thenReturn(true);

        when(mockNodeSettings.releaseVersion()).thenReturn("4.0.0");
        when(mockNodeSettings.partitioner()).thenReturn("org.apache.cassandra.dht.Murmur3Partitioner");
        // Setup instance metadata fetcher
        when(mockInstanceFetcher.callOnFirstAvailableInstance(any(Function.class)))
        .thenReturn(mockNodeSettings);

        // Setup database accessor
        when(mockDatabaseAccessor.fullSchema()).thenReturn(INITIAL_SCHEMA);
        when(mockDriverUnsupportedSchemaCache.getFullSchema()).thenReturn("");
        when(mockDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(mockDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenReturn(UUID.randomUUID());
        when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);

        clusterSchema = new CassandraClusterSchemaMonitor(
        mockInstanceFetcher,
        mockDatabaseAccessor,
        mockDriverUnsupportedSchemaCache,
        mockSidecarConfiguration,
        mockCassandraBridgeFactory
        );
    }

    @Test
    void testScheduleDecisionExecuteWhenBothConfigurationsEnabled()
    {
        when(mockCdcConfiguration.isEnabled()).thenReturn(true);
        when(mockSchemaKeyspaceConfiguration.isEnabled()).thenReturn(true);

        ScheduleDecision decision = clusterSchema.scheduleDecision();

        assertThat(decision).isEqualTo(ScheduleDecision.EXECUTE);
    }

    @Test
    void testScheduleDecisionSkipWhenCdcDisabled()
    {
        when(mockCdcConfiguration.isEnabled()).thenReturn(false);
        when(mockSchemaKeyspaceConfiguration.isEnabled()).thenReturn(true);

        ScheduleDecision decision = clusterSchema.scheduleDecision();

        assertThat(decision).isEqualTo(ScheduleDecision.SKIP);
    }

    @Test
    void testScheduleDecisionSkipWhenSchemaKeyspaceDisabled()
    {
        when(mockCdcConfiguration.isEnabled()).thenReturn(true);
        when(mockSchemaKeyspaceConfiguration.isEnabled()).thenReturn(false);

        ScheduleDecision decision = clusterSchema.scheduleDecision();

        assertThat(decision).isEqualTo(ScheduleDecision.SKIP);
    }

    @Test
    void testScheduleDecisionSkipWhenBothConfigurationsDisabled()
    {
        when(mockCdcConfiguration.isEnabled()).thenReturn(false);
        when(mockSchemaKeyspaceConfiguration.isEnabled()).thenReturn(false);

        ScheduleDecision decision = clusterSchema.scheduleDecision();

        assertThat(decision).isEqualTo(ScheduleDecision.SKIP);
    }

    @Test
    void testDelayReturnsCorrectInterval()
    {
        assertThat(clusterSchema.delay()).isEqualTo(DEFAULT_TABLE_SCHEMA_REFRESH_TIME);
    }

    @Test
    void testRefreshDetectsSchemaChangeAndUpdatesCdcTables()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls for initial schema
            Map<TableIdentifier, String> mockCreateStmts1 = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            Map<TableIdentifier, String> mockCreateStmts2 = Map.of(
            TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA,
            TableIdentifier.of("test", "another_cdc_table"), UPDATED_SCHEMA
            );
            cdcUtil.when(() -> CdcUtil.extractCdcTables(INITIAL_SCHEMA)).thenReturn(mockCreateStmts1);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(UPDATED_SCHEMA)).thenReturn(mockCreateStmts2);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            // First call returns initial schema, second call returns updated schema
            when(mockDatabaseAccessor.fullSchema())
            .thenReturn(INITIAL_SCHEMA)
            .thenReturn(UPDATED_SCHEMA);

            // First refresh - should detect and process schema
            clusterSchema.refresh();

            // Verify initial schema processing
            verify(mockDatabaseAccessor, times(1)).fullSchema();
            verify(mockDriverUnsupportedSchemaCache, times(1)).getFullSchema();
            verify(mockCdcBridge, times(1)).updateCdcSchema(any(Set.class), eq(Partitioner.Murmur3Partitioner), any());

            // Second refresh - should detect schema change and update
            clusterSchema.refresh();

            // Verify schema change detection and update
            verify(mockDatabaseAccessor, times(2)).fullSchema();
            verify(mockDriverUnsupportedSchemaCache, times(2)).getFullSchema();
            verify(mockCdcBridge, times(2)).updateCdcSchema(any(Set.class), eq(Partitioner.Murmur3Partitioner), any());
        }
    }

    @Test
    void testRefreshSkipsProcessingWhenSchemaUnchanged()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            // First refresh
            clusterSchema.refresh();
            verify(mockCdcBridge, times(1)).updateCdcSchema(any(Set.class), any(Partitioner.class), any());

            // Second refresh with same schema
            clusterSchema.refresh();

            // Should not call updateCdcSchema again since schema hasn't changed
            verify(mockDatabaseAccessor, times(2)).fullSchema();
            verify(mockCdcBridge, times(1)).updateCdcSchema(any(Set.class), any(Partitioner.class), any());
        }
    }

    @Test
    void testRefreshNotifiesSchemaChangeListeners()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            AtomicBoolean listener1Called = new AtomicBoolean(false);
            AtomicBoolean listener2Called = new AtomicBoolean(false);

            clusterSchema.addSchemaChangeListener(() -> listener1Called.set(true));
            clusterSchema.addSchemaChangeListener(() -> listener2Called.set(true));

            clusterSchema.refresh();

            assertThat(listener1Called.get()).isTrue();
            assertThat(listener2Called.get()).isTrue();
        }
    }

    @Test
    void testRefreshHandlesIllegalStateExceptionGracefully()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            when(mockDatabaseAccessor.fullSchema()).thenThrow(new IllegalStateException("Database not ready"));

            // Should now throw the exception instead of handling it gracefully
            try
            {
                clusterSchema.refresh();
                assertThat(false).as("Expected IllegalStateException to be thrown").isTrue();
            }
            catch (IllegalStateException e)
            {
                assertThat(e.getMessage()).isEqualTo("Database not ready");
            }

            verify(mockDatabaseAccessor, times(1)).fullSchema();
            // CdcBridge should not be called due to exception
        }
    }

    @Test
    void testRefreshHandlesGenericExceptionGracefully()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            when(mockDatabaseAccessor.fullSchema()).thenThrow(new RuntimeException("Unexpected error"));

            // Should now throw the exception instead of handling it gracefully
            try
            {
                clusterSchema.refresh();
                assertThat(false).as("Expected RuntimeException to be thrown").isTrue();
            }
            catch (RuntimeException e)
            {
                assertThat(e.getMessage()).isEqualTo("Unexpected error");
            }

            verify(mockDatabaseAccessor, times(1)).fullSchema();
            // CdcBridge should not be called due to exception
        }
    }

    @Test
    void testExecuteCompletesPromiseSuccessfully()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            @SuppressWarnings("unchecked")
            Promise<Void> promise = mock(Promise.class);

            clusterSchema.execute(promise);

            verify(promise, times(1)).tryComplete();
            verify(promise, never()).fail(any(Throwable.class));
        }
    }

    @Test
    void testExecuteFailsPromiseOnException()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            @SuppressWarnings("unchecked")
            Promise<Void> promise = mock(Promise.class);
            RuntimeException expectedException = new RuntimeException("Refresh failed");

            when(mockDatabaseAccessor.fullSchema()).thenThrow(expectedException);

            clusterSchema.execute(promise);

            ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
            verify(promise, times(1)).fail(throwableCaptor.capture());
            verify(promise, never()).tryComplete();

            assertThat(throwableCaptor.getValue()).isEqualTo(expectedException);
        }
    }

    @Test
    void testBuildCdcTablesWithDatabaseAccessor()
    {
        try (MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            ConcurrentHashMap<TableIdentifier, UUID> tableIdCache = new ConcurrentHashMap<>();
            UUID testTableId = UUID.randomUUID();

            when(mockDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenReturn(testTableId);
            when(mockDatabaseAccessor.fullSchema()).thenReturn(INITIAL_SCHEMA);

            Set<CqlTable> result = CassandraClusterSchemaMonitor.buildCdcTables(
            mockDatabaseAccessor,
            mockDriverUnsupportedSchemaCache,
            tableIdCache,
            mockCassandraBridge
            );

            assertThat(result).isNotNull();
            verify(mockDatabaseAccessor, times(1)).fullSchema();
            verify(mockDatabaseAccessor, times(1)).partitioner();
        }
    }

    @Test
    void testAddSchemaChangeListenerStoresListener()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            AtomicBoolean listenerCalled = new AtomicBoolean(false);
            Runnable listener = () -> listenerCalled.set(true);

            clusterSchema.addSchemaChangeListener(listener);
            clusterSchema.refresh();

            assertThat(listenerCalled.get()).isTrue();
        }
    }

    @Test
    void testMultipleSchemaChangeListenersAllNotified()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            AtomicBoolean listener1Called = new AtomicBoolean(false);
            AtomicBoolean listener2Called = new AtomicBoolean(false);
            AtomicBoolean listener3Called = new AtomicBoolean(false);

            clusterSchema.addSchemaChangeListener(() -> listener1Called.set(true));
            clusterSchema.addSchemaChangeListener(() -> listener2Called.set(true));
            clusterSchema.addSchemaChangeListener(() -> listener3Called.set(true));

            clusterSchema.refresh();

            assertThat(listener1Called.get()).isTrue();
            assertThat(listener2Called.get()).isTrue();
            assertThat(listener3Called.get()).isTrue();
        }
    }

    @Test
    void testSchemaChangeListenersNotCalledWhenNoSchemaChange()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            AtomicBoolean listenerCalled = new AtomicBoolean(false);
            clusterSchema.addSchemaChangeListener(() -> listenerCalled.set(true));

            // First refresh - should call listener
            clusterSchema.refresh();
            assertThat(listenerCalled.get()).isTrue();

            // Reset and refresh again with same schema
            listenerCalled.set(false);
            clusterSchema.refresh();
            assertThat(listenerCalled.get()).isFalse();
        }
    }

    @Test
    void testSchemaChangeListenersNotCalledOnException()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            AtomicBoolean listenerCalled = new AtomicBoolean(false);
            clusterSchema.addSchemaChangeListener(() -> listenerCalled.set(true));

            when(mockDatabaseAccessor.fullSchema()).thenThrow(new RuntimeException("Error"));

            // Should now throw the exception instead of handling it gracefully
            try
            {
                clusterSchema.refresh();
                assertThat(false).as("Expected RuntimeException to be thrown").isTrue();
            }
            catch (RuntimeException e)
            {
                assertThat(e.getMessage()).isEqualTo("Error");
            }

            assertThat(listenerCalled.get()).isFalse();
        }
    }

    @Test
    void testRefreshUpdatesTableIdCache()
    {
        try (MockedStatic<CdcBridgeFactory> cdcBridgeFactory = Mockito.mockStatic(CdcBridgeFactory.class);
             MockedStatic<CdcUtil> cdcUtil = Mockito.mockStatic(CdcUtil.class);
             MockedStatic<CqlUtils> cqlUtils = Mockito.mockStatic(CqlUtils.class))
        {
            when(mockCassandraBridgeFactory.get(anyString())).thenReturn(mockCassandraBridge);
            cdcBridgeFactory.when(() -> CdcBridgeFactory.getCdcBridge(any(CassandraBridge.class))).thenReturn(mockCdcBridge);

            // Mock utility class calls
            Map<TableIdentifier, String> mockCreateStmts = Collections.singletonMap(TableIdentifier.of("test", "cdc_table"), INITIAL_SCHEMA);
            cdcUtil.when(() -> CdcUtil.extractCdcTables(anyString())).thenReturn(mockCreateStmts);
            cqlUtils.when(() -> CqlUtils.extractUdts(anyString(), anyString())).thenReturn(Collections.emptySet());
            cqlUtils.when(() -> CqlUtils.extractReplicationFactor(anyString(), anyString())).thenReturn(ReplicationFactor.simpleStrategy(1));
            when(mockCassandraBridge.buildSchema(anyString(), anyString(), any(ReplicationFactor.class), any(Partitioner.class), any(Set.class), any(UUID.class), any(Integer.class), any(Boolean.class))).thenReturn(mock(CqlTable.class));

            TableIdentifier expectedTableId = TableIdentifier.of("test", "cdc_table");
            UUID expectedUuid = UUID.randomUUID();

            when(mockDatabaseAccessor.getTableId(expectedTableId)).thenReturn(expectedUuid);

            clusterSchema.refresh();

            // Verify that the database accessor was called to get table ID
            verify(mockDatabaseAccessor).getTableId(any(TableIdentifier.class));
        }
    }
}
