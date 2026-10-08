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

package org.apache.cassandra.sidecar.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;

import static org.apache.cassandra.sidecar.db.CQLSchemaAccessorTest.VIRTUAL_KEYSPACE;
import static org.apache.cassandra.sidecar.db.CQLSchemaAccessorTest.createKeyspaceStatement;
import static org.apache.cassandra.sidecar.db.CQLSchemaAccessorTest.keyspaceSchema;
import static org.apache.cassandra.sidecar.db.CQLSchemaAccessorTest.mockRow;
import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SchemaCacheTest
{
    static final Name KEYSPACE = new Name("keyspace1");
    static final Name TABLE = new Name("table1");
    static final String TABLE_LIST_QUERY = "SELECT keyspace_name, table_name FROM system_schema.tables";
    // a table the Java driver cannot parse, because of the vector data type
    static final String SCHEMA = "CREATE TABLE keyspace1.table1 (a int, b vector<float, 3>, PRIMARY KEY (a));";
    static final String KEYSPACE_SCHEMA = keyspaceSchema(KEYSPACE.name(), SCHEMA);

    CQLSessionProvider sessionProvider;
    Session session;
    SchemaCache schemaCache;
    List<Map<String, String>> tableRows;
    List<Map<String, String>> keyspaceRows;

    @BeforeEach
    void setUp()
    {
        sessionProvider = CQLSchemaAccessorTest.mockCQLSessionProvider(KEYSPACE.name(), TABLE.name(), SCHEMA);
        session = sessionProvider.get();
        tableRows = new ArrayList<>();
        tableRows.add(Map.of("keyspace_name", KEYSPACE.name(), "table_name", TABLE.name()));
        mockPreparedQuery(session, TABLE_LIST_QUERY, tableRows);
        keyspaceRows = new ArrayList<>();
        keyspaceRows.add(Map.of("keyspace_name", KEYSPACE.name()));
        mockQuery(session, "DESCRIBE KEYSPACES", keyspaceRows);
        SidecarConfiguration sidecarConfiguration = mockSidecarConfiguration();
        schemaCache = new SchemaCache(sidecarConfiguration, sessionProvider);
    }

    @Test
    void testImmediateUnsupportedTableSchemaLookup()
    {
        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(SCHEMA);
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE)).isEqualTo(SCHEMA);

        assertThat(schemaCache.getUnsupportedTableSchema(new Name("unknown"), TABLE)).isNull();
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, new Name("unknown"))).isNull();
    }

    @Test
    void testUnsupportedTableSchemaLookupWithoutRefresh()
    {
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE, false)).isEqualTo(SCHEMA);

        // for a table created after the last refresh
        Name table = new Name("table2");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, table, false)).isNull();
        verify(session, never()).execute("DESCRIBE TABLE keyspace1.table2");

        mockDescribeTable(session, KEYSPACE.name(), table.name(), "CREATE TABLE keyspace1.table2 (a int PRIMARY KEY);");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, table, true))
        .isEqualTo("CREATE TABLE keyspace1.table2 (a int PRIMARY KEY);");
        verify(session, times(1)).execute("DESCRIBE TABLE keyspace1.table2");
    }

    @Test
    void testUnsupportedTableSchemaLookupForUnquotedNames()
    {
        assertThat(schemaCache.getUnsupportedTableSchema(new Name("Keyspace1"),
                                                         new Name("TABLE1"),
                                                         false))
        .isEqualTo(SCHEMA);

        assertThat(schemaCache.getUnsupportedTableSchema(new Name("\"Keyspace1\""),
                                                         new Name("\"Table1\""),
                                                         false))
        .isNull();

        assertThat(schemaCache.getUnsupportedTableSchema(new Name("\"keyspace1\""),
                                                         new Name("\"table1\""),
                                                         false))
        .isEqualTo(SCHEMA);
    }

    @Test
    void testUnsupportedTableSchemaLookupAfterRefresh()
    {
        schemaCache.setInitialized(true);

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo("");

        Promise<Void> p = Promise.promise();
        schemaCache.execute(p);
        Future<Void> future = p.future();
        assertThat(future.succeeded()).isTrue();

        when(sessionProvider.get()).thenThrow(new CassandraUnavailableException(CQL, "CQL unavailable"));

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(SCHEMA);
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE)).isEqualTo(SCHEMA);
    }

    @Test
    void testUnsupportedTableSchemaIsAssembledInStableOrder()
    {
        String otherSchema = "CREATE TABLE keyspace1.table0 (a int, b vector<float, 3>, PRIMARY KEY (a));";
        mockDescribeTable(session, KEYSPACE.name(), "table0", otherSchema);
        tableRows.add(Map.of("keyspace_name", KEYSPACE.name(), "table_name", "table0"));

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(otherSchema + "\n\n" + SCHEMA);
        schemaCache.refresh(false);
        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(otherSchema + "\n\n" + SCHEMA);
    }

    @Test
    void testDroppedTableIsRemovedOnRefresh()
    {
        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(SCHEMA);
        tableRows.clear();
        schemaCache.refresh(false);
        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo("");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE, false)).isNull();
    }

    @Test
    void testTableKnownToDriverIsNotCached()
    {
        Cluster mockCluster = mock(Cluster.class);
        when(session.getCluster()).thenReturn(mockCluster);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        doReturn(List.of(mockKeyspaceMetadata(KEYSPACE.name(), TABLE.name()))).when(mockMetadata).getKeyspaces();

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo("");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE, false)).isNull();
    }

    @Test
    void testImmediateDescribeSchemaLookup()
    {
        assertThat(schemaCache.getFullSchema()).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema("unknown")).isNull();

        verify(session, times(1)).execute("DESCRIBE KEYSPACE " + KEYSPACE.name());
    }

    @Test
    void testDescribeSchemaLookupOnDemand()
    {
        schemaCache.setInitialized(true);
        assertThat(schemaCache.getFullSchema()).isEqualTo("");

        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
        verify(session, times(1)).execute("DESCRIBE KEYSPACE " + KEYSPACE.name());

        when(sessionProvider.get()).thenThrow(new CassandraUnavailableException(CQL, "CQL unavailable"));

        assertThat(schemaCache.getFullSchema()).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
    }

    @Test
    void testDescribeSchemaOfAllKeyspacesIsInvalidatedOnDemandLookup()
    {
        assertThat(schemaCache.getFullSchema()).isEqualTo(KEYSPACE_SCHEMA);

        String otherSchema = "CREATE TABLE keyspace2.table1 (a int PRIMARY KEY);";
        mockDescribeKeyspace(session, "keyspace2", otherSchema);

        assertThat(schemaCache.getKeyspaceSchema("keyspace2")).isEqualTo(keyspaceSchema("keyspace2", otherSchema));
        assertThat(schemaCache.getFullSchema())
        .isEqualTo(KEYSPACE_SCHEMA + "\n\n" + keyspaceSchema("keyspace2", otherSchema));
    }

    @Test
    void testDescribeSchemaIsAssembledInStableOrder()
    {
        String otherSchema = "CREATE TABLE aaa_keyspace.table1 (a int PRIMARY KEY);";
        mockDescribeKeyspace(session, "aaa_keyspace", otherSchema);
        keyspaceRows.add(Map.of("keyspace_name", "aaa_keyspace"));

        assertThat(schemaCache.getFullSchema())
        .isEqualTo(keyspaceSchema("aaa_keyspace", otherSchema) + "\n\n" + KEYSPACE_SCHEMA);
        schemaCache.refresh(false);
        assertThat(schemaCache.getFullSchema())
        .isEqualTo(keyspaceSchema("aaa_keyspace", otherSchema) + "\n\n" + KEYSPACE_SCHEMA);
    }

    @Test
    void testDroppedKeyspaceIsRemovedOnRefresh()
    {
        assertThat(schemaCache.getFullSchema()).isEqualTo(KEYSPACE_SCHEMA);

        keyspaceRows.clear();
        schemaCache.refresh(false);

        assertThat(schemaCache.getFullSchema()).isEqualTo("");
    }

    @Test
    void testKeyspaceWithoutSchemaIsNotCached()
    {
        keyspaceRows.add(Map.of("keyspace_name", VIRTUAL_KEYSPACE));

        assertThat(schemaCache.getFullSchema()).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(VIRTUAL_KEYSPACE)).isNull();
    }

    @Test
    void testLookupWhileCassandraIsUnavailable()
    {
        CQLSessionProvider unavailable = mock(CQLSessionProvider.class);
        when(unavailable.get()).thenThrow(new CassandraUnavailableException(CQL, "CQL unavailable"));
        SchemaCache cache = new SchemaCache(mockSidecarConfiguration(), unavailable);

        assertThat(cache.getFullSchema()).isEqualTo("");
        assertThat(cache.getUnsupportedTableSchema()).isEqualTo("");

        assertThatThrownBy(() -> cache.getKeyspaceSchema(KEYSPACE.name()))
        .isInstanceOf(CassandraUnavailableException.class);
        assertThatThrownBy(() -> cache.getUnsupportedTableSchema(KEYSPACE, TABLE))
        .isInstanceOf(CassandraUnavailableException.class);

        assertThat(cache.getUnsupportedTableSchema(KEYSPACE, TABLE, false)).isNull();
    }

    @Test
    void testExecuteFailsWhenRefreshFails()
    {
        CQLSessionProvider failing = mock(CQLSessionProvider.class);
        when(failing.get()).thenThrow(new RuntimeException("cannot get session"));
        SchemaCache cache = new SchemaCache(mockSidecarConfiguration(), failing);

        Promise<Void> p = Promise.promise();
        cache.execute(p);
        Future<Void> future = p.future();
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(RuntimeException.class).hasMessage("cannot get session");
    }

    @Test
    void testConcatSchemas()
    {
        assertThat(SchemaCache.concatSchemas()).isEqualTo("");
        assertThat(SchemaCache.concatSchemas("", null)).isEqualTo("");
        assertThat(SchemaCache.concatSchemas(SCHEMA)).isEqualTo(SCHEMA);
        assertThat(SchemaCache.concatSchemas(KEYSPACE_SCHEMA, null, "", "\n" + SCHEMA + "\n"))
        .isEqualTo(KEYSPACE_SCHEMA + "\n\n" + SCHEMA);
    }

    static SidecarConfiguration mockSidecarConfiguration()
    {
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class);
        DriverConfiguration driverConfiguration = mock(DriverConfiguration.class);
        when(driverConfiguration.schemaRefreshTime()).thenReturn(new SecondBoundConfiguration(5, TimeUnit.SECONDS));
        when(sidecarConfiguration.driverConfiguration()).thenReturn(driverConfiguration);
        return sidecarConfiguration;
    }

    static void mockPreparedQuery(Session session, String statement, List<Map<String, String>> rows)
    {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(eq(statement))).thenReturn(preparedStatement);
        when(preparedStatement.bind()).thenReturn(boundStatement);
        when(session.execute(eq(boundStatement))).then(invocation -> mockResultSet(rows));
    }

    static void mockDescribeKeyspace(Session session, String keyspace, String schema)
    {
        mockQuery(session,
                  "DESCRIBE KEYSPACE " + keyspace,
                  List.of(Map.of("create_statement", createKeyspaceStatement(keyspace)),
                          Map.of("create_statement", schema)));
    }

    static void mockDescribeTable(Session session, String keyspace, String table, String schema)
    {
        mockQuery(session,
                  String.format("DESCRIBE TABLE %s.%s", keyspace, table),
                  List.of(Map.of("create_statement", schema)));
    }

    static void mockQuery(Session session, String statement, List<Map<String, String>> rows)
    {
        when(session.execute(eq(statement))).then(invocation -> mockResultSet(rows));
    }

    static KeyspaceMetadata mockKeyspaceMetadata(String keyspace, String table)
    {
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getName()).thenReturn(table);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(keyspace);
        when(keyspaceMetadata.getTables()).thenReturn(List.of(tableMetadata));
        return keyspaceMetadata;
    }

    static ResultSet mockResultSet(List<Map<String, String>> rows)
    {
        ResultSet resultSet = mock(ResultSet.class);
        List<Row> mockRows = new ArrayList<>();
        rows.forEach(row -> mockRows.add(mockRow(row)));
        when(resultSet.all()).thenReturn(mockRows);
        return resultSet;
    }
}
