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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.vertx.core.Vertx;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.TableIdLookup;
import org.apache.cassandra.cdc.avro.AvroSchemas;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.cdc.schemastore.SchemaStorePublisherFactory;
import org.apache.cassandra.cdc.schemastore.TableSchemaPublisher;
import org.apache.cassandra.cdc.sidecar.SidecarCdcStats;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.db.TableHistoryDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.tasks.CassandraClusterSchemaMonitor;
import org.apache.cassandra.sidecar.utils.TokenSplitUtil;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CONFIGURATION_CHANGED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_START;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for Caching Schema Store
 */
public class CachingSchemaStoreTest
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE = "test_table";
    private static final String CREATE_STATEMENT = "CREATE TABLE " + TEST_KEYSPACE + '.' + TEST_TABLE + " (\n" +
                                                   "    a bigint PRIMARY KEY,\n" +
                                                   "    b text\n" +
                                                   ") WITH cdc = true;";
    private static final ReplicationFactor REPLICATION_FACTOR = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                                                      Map.of("DC1", 3));
    private static final Partitioner PARTITIONER = Partitioner.Murmur3Partitioner;

    CassandraBridge cassandraBridge;

    final Vertx vertx = Vertx.vertx();
    SidecarSchema mockSidecarSchema;
    CassandraClusterSchemaMonitor mockCassandraClusterSchemaMonitor;
    CachingSchemaStore cachingSchemaStore;
    CdcConfigImpl mockCdcConfig;
    SidecarCdcStats mockSidecarCdcStats;
    TableHistoryDatabaseAccessor spyTableHistoryDatabaseAccessor;
    CqlToAvroSchemaConverter cqlToAvroSchemaConverter;

    private void setupForVersion(CassandraVersion version)
    {
        TokenSplitUtil tokenSplitUtil = new TokenSplitUtil(32);

        mockSidecarSchema = mock(SidecarSchema.class);
        when(mockSidecarSchema.isInitialized()).thenReturn(true);

        cassandraBridge = new CassandraBridgeFactory().get(version);
        cqlToAvroSchemaConverter = CdcBridgeFactory.getCqlToAvroSchemaConverter(cassandraBridge);

        mockCassandraClusterSchemaMonitor = mock(CassandraClusterSchemaMonitor.class);

        mockCdcConfig = mock(CdcConfigImpl.class);

        TableHistoryDatabaseAccessor tableHistoryDatabaseAccessor = new TableHistoryDatabaseAccessor(mockSidecarSchema, null);
        spyTableHistoryDatabaseAccessor = spy(tableHistoryDatabaseAccessor);
        doReturn(null).when(spyTableHistoryDatabaseAccessor).insertTableSchemaHistory(anyString(), anyString(), anyString());

        mockSidecarCdcStats = mock(SidecarCdcStats.class);
    }

    @ParameterizedTest
    @EnumSource(value = CassandraVersion.class, names = {"FOURZERO", "FOURONE" /*, "FIVEZERO" is having problems*/})
    public void testCachingSchemaStore(CassandraVersion version)
    {
        setupForVersion(version);
        Set<CqlTable> initialTables = cqlTables(CREATE_STATEMENT);
        mockCassandraClusterSchemaMonitor = createMockClusterSchema(initialTables);
        cachingSchemaStore = createCachingSchemaStore(mockCassandraClusterSchemaMonitor);

        // Schema
        Schema schema = cachingSchemaStore.getSchema(TEST_KEYSPACE + "." + TEST_TABLE, "");
        verifyFieldType(schema.getField("a").schema(), Schema.Type.LONG, "bigint");
        verifyFieldType(schema.getField("b").schema(), Schema.Type.STRING, "text");

        // Writer
        GenericDatumWriter<GenericRecord> writer = cachingSchemaStore.getWriter(TEST_KEYSPACE + "." + TEST_TABLE, "");
        assertNotNull(writer);

        // Reader
        GenericDatumReader<GenericRecord> reader = cachingSchemaStore.getReader(TEST_KEYSPACE + "." + TEST_TABLE, "");
        assertNotNull(reader);
    }

    @ParameterizedTest
    @EnumSource(value = CassandraVersion.class, names = {"FOURZERO", "FOURONE" /*, "FIVEZERO" is having problems*/})
    public void testCachingSchemaStoreWithSidecarSchemaEnabled(CassandraVersion version)
    {
        setupForVersion(version);
        String newTableName = "new_test_table";
        String newSchema = modifyTableName(CREATE_STATEMENT, newTableName);
        Set<CqlTable> initialTables = cqlTables(CREATE_STATEMENT);

        Set<CqlTable> addedTables = new HashSet<>(cqlTables(newSchema));
        addedTables.addAll(initialTables);
        mockCassandraClusterSchemaMonitor = createMockClusterSchema(initialTables, addedTables);

        cachingSchemaStore = createCachingSchemaStore(mockCassandraClusterSchemaMonitor);
        cachingSchemaStore.onSchemaChanged();

        verify(spyTableHistoryDatabaseAccessor, times(1)).insertTableSchemaHistory(eq(TEST_KEYSPACE), eq(newTableName), any());
    }

    @ParameterizedTest
    @EnumSource(value = CassandraVersion.class, names = {"FOURZERO", "FOURONE" /*, "FIVEZERO" is having problems*/})
    public void testCachingSchemaStoreUnKnownTable(CassandraVersion version)
    {
        setupForVersion(version);
        String newTableName = "new_test_table";
        String newSchema = modifyTableName(CREATE_STATEMENT, newTableName);
        Set<CqlTable> initialTables = cqlTables(CREATE_STATEMENT);

        Set<CqlTable> addedTables = new HashSet<>(cqlTables(newSchema));
        addedTables.addAll(initialTables);
        mockCassandraClusterSchemaMonitor = createMockClusterSchema(initialTables, addedTables);

        cachingSchemaStore = createCachingSchemaStore(mockCassandraClusterSchemaMonitor);

        assertUnknownTableException("unknown", "schema");
        assertUnknownTableException("unknown", "writer");
        assertUnknownTableException("unknown", "reader");
    }

    @ParameterizedTest
    @EnumSource(value = CassandraVersion.class, names = {"FOURZERO", "FOURONE" /*, "FIVEZERO" is having problems*/})
    public void testCachingSchemaStoreSchemaNotChanged(CassandraVersion version)
    {
        setupForVersion(version);
        String newTableName = "new_test_table";
        String newSchema = modifyTableName(CREATE_STATEMENT, newTableName);
        Set<CqlTable> initialTables = cqlTables(CREATE_STATEMENT);

        Set<CqlTable> addedTables = new HashSet<>(cqlTables(newSchema));
        addedTables.addAll(initialTables);
        mockCassandraClusterSchemaMonitor = createMockClusterSchema(initialTables, addedTables);

        cachingSchemaStore = createCachingSchemaStore(mockCassandraClusterSchemaMonitor);

        // Should return cached schema object when schema is not changed
        assertSchemaCached(TEST_TABLE);
    }

    @ParameterizedTest
    @EnumSource(value = CassandraVersion.class, names = {"FOURZERO", "FOURONE" /*, "FIVEZERO" is having problems*/})
    public void testCachingSchemaStoreNewTableAdded(CassandraVersion version)
    {
        setupForVersion(version);
        String newTable = "new_test_table";
        String newSchema = modifyTableName(CREATE_STATEMENT, newTable);
        Set<CqlTable> initialTables = cqlTables(CREATE_STATEMENT);
        Set<CqlTable> addedTables = new HashSet<>(cqlTables(newSchema));
        addedTables.addAll(initialTables);

        mockCassandraClusterSchemaMonitor = createMockClusterSchema(initialTables, addedTables);
        cachingSchemaStore = createCachingSchemaStore(mockCassandraClusterSchemaMonitor);

        assertUnknownTableException(newTable, "schema");
        assertUnknownTableException(newTable, "writer");
        assertUnknownTableException(newTable, "reader");

        cachingSchemaStore.onSchemaChanged();

        assertSchemaAccessible(newTable);
    }

    @ParameterizedTest
    @EnumSource(value = CassandraVersion.class, names = {"FOURZERO", "FOURONE" /*, "FIVEZERO" is having problems*/})
    public void testCachingSchemaStoreTableDeleted(CassandraVersion version)
    {
        setupForVersion(version);
        Set<CqlTable> tables = cqlTables(CREATE_STATEMENT);
        mockCassandraClusterSchemaMonitor = createMockClusterSchema(tables, Collections.emptySet());

        cachingSchemaStore = createCachingSchemaStore(mockCassandraClusterSchemaMonitor);
        assertSchemaAccessible(TEST_TABLE);

        cachingSchemaStore.onSchemaChanged();

        assertUnknownTableException(TEST_TABLE, "schema");
        assertUnknownTableException(TEST_TABLE, "writer");
        assertUnknownTableException(TEST_TABLE, "reader");
    }

    @ParameterizedTest
    @EnumSource(value = CassandraVersion.class, names = {"FOURZERO", "FOURONE" /*, "FIVEZERO" is having problems*/})
    public void testCachingSchemaStoreTableSchemaChanged(CassandraVersion version)
    {
        setupForVersion(version);
        String changedSchema = "CREATE TABLE test_keyspace.test_table (\n" +
                               "    a bigint PRIMARY KEY,\n" +
                               "    b text,\n" +
                               "    c bigint\n" +
                               ") WITH additional_write_policy = '99p'\n" +
                               "    AND cdc = true;";

        Set<CqlTable> initialTables = cqlTables(CREATE_STATEMENT);
        mockCassandraClusterSchemaMonitor = createMockClusterSchema(initialTables);
        cachingSchemaStore = createCachingSchemaStore(mockCassandraClusterSchemaMonitor);

        Schema schema1 = cachingSchemaStore.getSchema(TEST_KEYSPACE + "." + TEST_TABLE, "");
        GenericDatumReader<GenericRecord> reader1 = cachingSchemaStore.getReader(TEST_KEYSPACE + "." + TEST_TABLE, "");
        GenericDatumWriter<GenericRecord> writer1 = cachingSchemaStore.getWriter(TEST_KEYSPACE + "." + TEST_TABLE, "");


        // Update mock to return changed schema
        Set<CqlTable> changedTables = cqlTables(changedSchema);
        when(mockCassandraClusterSchemaMonitor.getCdcTables()).thenReturn(changedTables);

        cachingSchemaStore.onSchemaChanged();

        Schema schema2 = cachingSchemaStore.getSchema(TEST_KEYSPACE + "." + TEST_TABLE, "");
        GenericDatumReader<GenericRecord> reader2 = cachingSchemaStore.getReader(TEST_KEYSPACE + "." + TEST_TABLE, "");
        GenericDatumWriter<GenericRecord> writer2 = cachingSchemaStore.getWriter(TEST_KEYSPACE + "." + TEST_TABLE, "");

        assertNotEquals(schema1, schema2);
        assertNotEquals(reader1, reader2);
        assertNotEquals(writer1, writer2);
    }

    /**
     * A bad/missing Schema Store config at boot leaves {@code publisher} null (the failure is
     * logged and swallowed, per {@code reloadPublisherAndPublishSchemas}). Regression test for
     * the gap where fixing the {@code configs} table alone never re-triggered the load — only an
     * unrelated CQL schema change did. Reacting to {@code ON_CDC_CONFIGURATION_CHANGED} closes
     * that gap.
     */
    @Test
    void testOnCdcConfigurationChangedReloadsSchemaStorePublisher() throws InterruptedException
    {
        setupForVersion(CassandraVersion.FOURZERO);
        when(mockCdcConfig.cdcEnabled()).thenReturn(true);

        AtomicInteger buildPublisherCalls = new AtomicInteger(0);
        SchemaStorePublisherFactory countingPublisherFactory = kafkaOptions -> {
            buildPublisherCalls.incrementAndGet();
            return mock(TableSchemaPublisher.class);
        };

        Set<CqlTable> tables = cqlTables(CREATE_STATEMENT);
        mockCassandraClusterSchemaMonitor = createMockClusterSchema(tables);

        Vertx testVertx = Vertx.vertx();
        try
        {
            cachingSchemaStore = new CachingSchemaStore(testVertx, mockCassandraClusterSchemaMonitor,
                                                        spyTableHistoryDatabaseAccessor, mockCdcConfig,
                                                        mockSidecarCdcStats, mockSidecarSchema,
                                                        cqlToAvroSchemaConverter, countingPublisherFactory);

            // ON_SERVER_START registers the ON_CDC_CONFIGURATION_CHANGED consumer, mirroring real startup.
            testVertx.eventBus().publish(ON_SERVER_START.address(), "server started");
            assertThat(buildPublisherCalls.get()).isEqualTo(0);

            // Registration above is async, so retry the publish until the consumer picks it up.
            loopAssert(5, () -> {
                if (buildPublisherCalls.get() == 0)
                {
                    testVertx.eventBus().publish(ON_CDC_CONFIGURATION_CHANGED.address(), "Cdc Configuration Changed");
                }
                assertThat(buildPublisherCalls.get()).isEqualTo(1);
            });

            // Consumer is registered now, so a second config-change event should trigger another reload.
            testVertx.eventBus().publish(ON_CDC_CONFIGURATION_CHANGED.address(), "Cdc Configuration Changed");
            loopAssert(5, () -> assertThat(buildPublisherCalls.get()).isEqualTo(2));
        }
        finally
        {
            testVertx.close();
        }
    }

    public Set<CqlTable> cqlTables(String createStatement)
    {
        Set<CqlTable> tables = Set.of(cassandraBridge.buildSchema(createStatement, TEST_KEYSPACE, REPLICATION_FACTOR, PARTITIONER));
        CdcBridge cdcBridge = CdcBridgeFactory.getCdcBridge(cassandraBridge);
        cdcBridge.updateCdcSchema(tables, PARTITIONER, TableIdLookup.STUB);
        return Set.of(cassandraBridge.buildSchema(createStatement, TEST_KEYSPACE, REPLICATION_FACTOR, PARTITIONER));
    }

    private CachingSchemaStore createCachingSchemaStore(CassandraClusterSchemaMonitor clusterSchema)
    {
        return new CachingSchemaStore(vertx, clusterSchema, spyTableHistoryDatabaseAccessor,
                                      mockCdcConfig, mockSidecarCdcStats,
                                      mockSidecarSchema, cqlToAvroSchemaConverter,
                                      SchemaStorePublisherFactory.DEFAULT);
    }

    private CassandraClusterSchemaMonitor createMockClusterSchema(Set<CqlTable> tables)
    {
        CassandraClusterSchemaMonitor mock = mock(CassandraClusterSchemaMonitor.class);
        when(mock.getCdcTables()).thenReturn(tables);
        return mock;
    }

    private CassandraClusterSchemaMonitor createMockClusterSchema(Set<CqlTable> initialTables, Set<CqlTable> changedTables)
    {
        CassandraClusterSchemaMonitor mock = mock(CassandraClusterSchemaMonitor.class);
        when(mock.getCdcTables()).thenReturn(initialTables).thenReturn(changedTables);
        return mock;
    }

    private void assertUnknownTableException(String tableName, String operation)
    {
        String expectedMessage = String.format("Unable to get %s for unknown table TableIdentifier{keyspace='%s', table='%s'}",
                                                operation, TEST_KEYSPACE, tableName);
        switch (operation)
        {
            case "schema":
                assertThatRuntimeException()
                    .isThrownBy(() -> cachingSchemaStore.getSchema(TEST_KEYSPACE + "." + tableName, ""))
                    .withMessage(expectedMessage);
                break;
            case "writer":
                assertThatRuntimeException()
                    .isThrownBy(() -> cachingSchemaStore.getWriter(TEST_KEYSPACE + "." + tableName, ""))
                    .withMessage(expectedMessage);
                break;
            case "reader":
                assertThatRuntimeException()
                    .isThrownBy(() -> cachingSchemaStore.getReader(TEST_KEYSPACE + "." + tableName, ""))
                    .withMessage(expectedMessage);
                break;
        }
    }

    private void assertSchemaAccessible(String tableName)
    {
        assertNotNull(cachingSchemaStore.getSchema(TEST_KEYSPACE + "." + tableName, ""));
        assertNotNull(cachingSchemaStore.getWriter(TEST_KEYSPACE + "." + tableName, ""));
        assertNotNull(cachingSchemaStore.getReader(TEST_KEYSPACE + "." + tableName, ""));
    }

    private void assertSchemaCached(String tableName)
    {
        assertSame(cachingSchemaStore.getSchema(TEST_KEYSPACE + "." + tableName, ""),
                   cachingSchemaStore.getSchema(TEST_KEYSPACE + "." + tableName, ""));
        assertSame(cachingSchemaStore.getWriter(TEST_KEYSPACE + "." + tableName, ""),
                   cachingSchemaStore.getWriter(TEST_KEYSPACE + "." + tableName, ""));
        assertSame(cachingSchemaStore.getReader(TEST_KEYSPACE + "." + tableName, ""),
                   cachingSchemaStore.getReader(TEST_KEYSPACE + "." + tableName, ""));
    }

    private String modifyTableName(String createStatement, String newTableName)
    {
        return createStatement.replace(TEST_TABLE, newTableName);
    }

    private void verifyFieldType(Schema fieldSchema, Schema.Type expectedType, String expectedCqlType)
    {
        Schema schema = fieldSchema.getTypes().stream().filter(Objects::nonNull).findAny().get();
        assertEquals(expectedType, schema.getType());
        assertEquals(expectedCqlType, AvroSchemas.cqlType(schema));
    }
}
