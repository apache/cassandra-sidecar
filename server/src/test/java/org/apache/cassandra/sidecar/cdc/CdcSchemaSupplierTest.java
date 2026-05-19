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

        cdcSchemaSupplier = new CdcSchemaSupplier(
            instanceMetadataFetcher,
            cassandraBridgeFactory,
            cdcDatabaseAccessor
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

        CompletableFuture<Set<CqlTable>> result = cdcSchemaSupplier.getCdcEnabledTables();

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

        CompletableFuture<Set<CqlTable>> result = cdcSchemaSupplier.getCdcEnabledTables();

        assertThat(result).isCompleted();
        Set<CqlTable> tables = result.get();
        assertThat(tables).isNotNull();
    }

    @Test
    void testGetCdcEnabledTablesWithNoCdcTables() throws ExecutionException, InterruptedException
    {
        NodeSettings nodeSettings = mockNodeSettings("4.1.0", "Murmur3Partitioner");

        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any()))
            .thenReturn(SCHEMA_NO_CDC)  // Schema with no CDC tables
            .thenReturn(nodeSettings);

        CassandraBridge realBridge = new CassandraBridgeFactory().get(CassandraVersion.FOURONE);
        when(cassandraBridgeFactory.get(anyString())).thenReturn(realBridge);
        when(cdcDatabaseAccessor.partitioner()).thenReturn(Partitioner.Murmur3Partitioner);

        CompletableFuture<Set<CqlTable>> result = cdcSchemaSupplier.getCdcEnabledTables();

        assertThat(result).isCompleted();
        Set<CqlTable> tables = result.get();
        assertThat(tables).isNotNull();
        assertThat(tables).isEmpty();
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

        cdcSchemaSupplier.getCdcEnabledTables();

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
