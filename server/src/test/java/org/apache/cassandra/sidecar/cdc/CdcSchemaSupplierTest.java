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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CdcSchemaSupplier
 */
public class CdcSchemaSupplierTest
{
    @Mock
    private InstanceMetadataFetcher instanceMetadataFetcher;
    @Mock
    private CassandraBridgeFactory cassandraBridgeFactory;
    @Mock
    private CdcDatabaseAccessor cdcDatabaseAccessor;
    @Mock
    private CassandraBridge cassandraBridge;
    @Mock
    private CdcBridge cdcBridge;
    @Mock
    private SidecarConfiguration sidecarConfiguration;
    @Mock
    private ServiceConfiguration serviceConfiguration;
    @Mock
    private CdcConfiguration cdcConfiguration;

    private CdcSchemaSupplier cdcSchemaSupplier;

    private static final String SAMPLE_SCHEMA =
        "CREATE KEYSPACE test_keyspace WITH REPLICATION = {'class':'NetworkTopologyStrategy','DC1':'3'} AND DURABLE_WRITES = true;\n" +
        "CREATE TABLE test_keyspace.cdc_table (\n" +
        "    id uuid PRIMARY KEY,\n" +
        "    name text,\n" +
        "    value int\n" +
        ") WITH cdc = true;\n";

    private static final String SCHEMA_NO_CDC =
        "CREATE KEYSPACE test_keyspace WITH REPLICATION = {'class':'NetworkTopologyStrategy','DC1':'3'} AND DURABLE_WRITES = true;\n" +
        "CREATE TABLE test_keyspace.regular_table (\n" +
        "    id uuid PRIMARY KEY,\n" +
        "    data text\n" +
        ");\n";

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.openMocks(this);

        when(sidecarConfiguration.serviceConfiguration()).thenReturn(serviceConfiguration);
        when(serviceConfiguration.cdcConfiguration()).thenReturn(cdcConfiguration);
        when(cdcConfiguration.batchStatementsEnabled()).thenReturn(true);

        cdcSchemaSupplier = new CdcSchemaSupplier(
            instanceMetadataFetcher,
            cassandraBridgeFactory,
            cdcDatabaseAccessor,
            sidecarConfiguration
        );
    }

    @Test
    void testGetCdcEnabledTablesReturnsCompletedFuture() throws ExecutionException, InterruptedException
    {
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "org.apache.cassandra.dht.Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(SAMPLE_SCHEMA)  // First call returns schema
            .thenReturn(nodeSettings);   // Second call returns node settings

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenReturn(UUID.randomUUID());

        CompletableFuture<Set<CqlTable>> result = cdcSchemaSupplier.getTables();

        assertThat(result).isNotNull();
        assertThat(result.isDone()).isTrue();
        assertThat(result.isCompletedExceptionally()).isFalse();

        Set<CqlTable> tables = result.get();
        assertThat(tables).isNotNull();
    }

    @Test
    void testGetCdcEnabledTablesWithMurmur3Partitioner() throws ExecutionException, InterruptedException
    {
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(SAMPLE_SCHEMA)
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenReturn(UUID.randomUUID());

        CompletableFuture<Set<CqlTable>> result = cdcSchemaSupplier.getTables();

        assertThat(result).isCompleted();
        Set<CqlTable> tables = result.get();
        assertThat(tables).isNotNull();
    }

    @Test
    void testGetTablesReturnsEmptyWhenSchemaHasNoCdcTables() throws ExecutionException, InterruptedException
    {
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(SCHEMA_NO_CDC)  // Schema with no CDC tables
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenReturn(UUID.randomUUID());
        // setUp() already configures batchStatementsEnabled=true — with no CDC-enabled table in
        // the schema at all, there is nothing to be "at risk" of a batch with, so no non-CDC
        // table is registered either.

        CompletableFuture<Set<CqlTable>> result = cdcSchemaSupplier.getTables();

        assertThat(result).isCompleted();
        assertThat(result.get()).isEmpty();
    }

    @Test
    void testGetTablesExcludesKeyspaceWithNoCdcTablesUnderCdcKeyspacesScope() throws ExecutionException, InterruptedException
    {
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(SCHEMA_NO_CDC)  // Keyspace has zero CDC-enabled tables
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenReturn(UUID.randomUUID());

        CompletableFuture<Set<CqlTable>> result = cdcSchemaSupplier.getTables();

        // A keyspace with no CDC-enabled tables is excluded entirely.
        assertThat(result.get()).isEmpty();
    }

    @Test
    void testGetTablesIncludesNonCdcTableWithMatchingPartitionKeyStructure() throws ExecutionException, InterruptedException
    {
        String mixedSchema =
            "CREATE KEYSPACE test_keyspace WITH REPLICATION = {'class':'NetworkTopologyStrategy','DC1':'3'} AND DURABLE_WRITES = true;\n" +
            "CREATE TABLE test_keyspace.cdc_table (id uuid PRIMARY KEY, data text) WITH cdc = true;\n" +
            "CREATE TABLE test_keyspace.regular_table (id uuid PRIMARY KEY, data text);\n";
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(mixedSchema)
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        // A distinct UUID per table — a single fixed UUID for all tables collides in the
        // bridge's schema-building code once more than one table is built in the same call.
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenAnswer(invocation -> UUID.randomUUID());

        Set<CqlTable> tables = cdcSchemaSupplier.getTables().get();

        // Both tables have a single-column uuid partition key — the same structure — so the
        // non-CDC table could be batched with the CDC table under a shared partition key and
        // must be registered too.
        assertThat(tables).hasSize(2);
        assertThat(tables).extracting(CqlTable::table).containsExactlyInAnyOrder("cdc_table", "regular_table");
    }

    @Test
    void testGetTablesExcludesNonCdcTableWithMismatchedPartitionKeyStructure() throws ExecutionException, InterruptedException
    {
        String mixedSchema =
            "CREATE KEYSPACE test_keyspace WITH REPLICATION = {'class':'NetworkTopologyStrategy','DC1':'3'} AND DURABLE_WRITES = true;\n" +
            "CREATE TABLE test_keyspace.cdc_table (id uuid PRIMARY KEY, data text) WITH cdc = true;\n" +
            "CREATE TABLE test_keyspace.regular_table (id text PRIMARY KEY, data text);\n";
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(mixedSchema)
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenAnswer(invocation -> UUID.randomUUID());

        Set<CqlTable> tables = cdcSchemaSupplier.getTables().get();

        // regular_table's partition key is text, not uuid — it could never be co-located with
        // cdc_table's update in the same Mutation, so it is excluded entirely.
        assertThat(tables).hasSize(1);
        assertThat(tables.iterator().next().table()).isEqualTo("cdc_table");
    }

    @Test
    void testGetTablesRegistersOnlyCdcTableWhenBatchStatementsDisabled() throws ExecutionException, InterruptedException
    {
        when(cdcConfiguration.batchStatementsEnabled()).thenReturn(false);
        String mixedSchema =
            "CREATE KEYSPACE test_keyspace WITH REPLICATION = {'class':'NetworkTopologyStrategy','DC1':'3'} AND DURABLE_WRITES = true;\n" +
            "CREATE TABLE test_keyspace.cdc_table (id uuid PRIMARY KEY, data text) WITH cdc = true;\n" +
            "CREATE TABLE test_keyspace.regular_table (id uuid PRIMARY KEY, data text);\n";
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(mixedSchema)
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenAnswer(invocation -> UUID.randomUUID());

        Set<CqlTable> tables = cdcSchemaSupplier.getTables().get();

        // Even though regular_table shares partition key structure with cdc_table, the operator
        // has asserted no cross-table batches happen — only the CDC-enabled table is registered.
        assertThat(tables).hasSize(1);
        assertThat(tables.iterator().next().table()).isEqualTo("cdc_table");
    }

    @Test
    void testGetCdcEnabledTablesCallsCassandraBridgeFactory()
    {
        String releaseVersion = "4.1.0";
        NodeSettings nodeSettings = mockNodeSettings(releaseVersion, "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(SAMPLE_SCHEMA)
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);
        when(cdcDatabaseAccessor.getTableId(any(TableIdentifier.class))).thenReturn(UUID.randomUUID());

        cdcSchemaSupplier.getTables();

        verify(cassandraBridgeFactory).get(releaseVersion);
    }

    private NodeSettings mockNodeSettings(String releaseVersion, String partitioner)
    {
        NodeSettings nodeSettings = mock(NodeSettings.class);
        when(nodeSettings.releaseVersion()).thenReturn(releaseVersion);
        when(nodeSettings.partitioner()).thenReturn(partitioner);
        return nodeSettings;
    }
}
