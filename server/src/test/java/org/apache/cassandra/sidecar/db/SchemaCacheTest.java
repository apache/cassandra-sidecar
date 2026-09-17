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
import com.datastax.driver.core.KeyspaceMetadata;
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
    // the rows Cassandra is mocked to return, mutable so that a test can simulate schema changes between refreshes
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
        // the cache is populated upon the first lookup, when the periodic refresh has not run yet
        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(SCHEMA);
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE)).isEqualTo(SCHEMA);

        assertThat(schemaCache.getUnsupportedTableSchema(new Name("unknown"), TABLE)).isNull();
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, new Name("unknown"))).isNull();
    }

    @Test
    void testUnsupportedTableSchemaLookupWithoutRefresh()
    {
        // the cache is populated upon the first lookup, no matter whether the lookup allows a refresh
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE, false)).isEqualTo(SCHEMA);

        // a table missing from the cache is not described when the lookup does not allow a refresh, so a table
        // created after the last refresh is reported as non-existent
        Name table = new Name("table2");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, table, false)).isNull();
        verify(session, never()).execute("DESCRIBE TABLE keyspace1.table2");

        // while it is described when the lookup allows a refresh
        mockDescribeTable(session, KEYSPACE.name(), table.name(), "CREATE TABLE keyspace1.table2 (a int PRIMARY KEY);");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, table, true))
        .isEqualTo("CREATE TABLE keyspace1.table2 (a int PRIMARY KEY);");
        verify(session, times(1)).execute("DESCRIBE TABLE keyspace1.table2");
    }

    @Test
    void testUnsupportedTableSchemaLookupAfterRefresh()
    {
        // mark cache as initialized, but effectively empty
        schemaCache.setInitialized(true);

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo("");

        Promise<Void> p = Promise.promise();
        schemaCache.execute(p);
        Future<Void> future = p.future();
        assertThat(future.succeeded()).isTrue();

        // simulate Cassandra unavailability to verify data is taken from cache
        when(sessionProvider.get()).thenThrow(new CassandraUnavailableException(CQL, "CQL unavailable"));

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(SCHEMA);
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE)).isEqualTo(SCHEMA);
    }

    @Test
    void testUnsupportedTableSchemaIsAssembledInStableOrder()
    {
        // the tables are queried into a set, so the cache has to keep them sorted for the assembled schema
        // to be repeatable
        String otherSchema = "CREATE TABLE keyspace1.table0 (a int, b vector<float, 3>, PRIMARY KEY (a));";
        mockDescribeTable(session, KEYSPACE.name(), "table0", otherSchema);
        tableRows.add(Map.of("keyspace_name", KEYSPACE.name(), "table_name", "table0"));

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(otherSchema + "\n\n" + SCHEMA);
    }

    @Test
    void testDroppedTableIsRemovedOnRefresh()
    {
        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo(SCHEMA);

        // the table is dropped, so Cassandra no longer lists it
        tableRows.clear();
        schemaCache.refresh(false);

        // the cache is replaced on every refresh, therefore the dropped table is gone
        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo("");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE, false)).isNull();
    }

    @Test
    void testTableKnownToDriverIsNotCached()
    {
        // the table is parseable by the Java driver, hence it is served from driver metadata and not cached here
        when(session.getCluster().getMetadata().getKeyspaces())
        .thenReturn(List.of(mockKeyspaceMetadata(KEYSPACE.name(), TABLE.name())));

        assertThat(schemaCache.getUnsupportedTableSchema()).isEqualTo("");
        assertThat(schemaCache.getUnsupportedTableSchema(KEYSPACE, TABLE, false)).isNull();
    }

    @Test
    void testImmediateDescribedSchemaLookup()
    {
        assertThat(schemaCache.getSchema()).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema("unknown")).isNull();

        // the keyspace is described once, during the refresh triggered by the first lookup, and served from
        // the cache afterwards
        verify(session, times(1)).execute("DESCRIBE KEYSPACE " + KEYSPACE.name());
    }

    @Test
    void testDescribedSchemaLookupOnDemand()
    {
        // mark cache as initialized, but effectively empty
        schemaCache.setInitialized(true);
        assertThat(schemaCache.getSchema()).isEqualTo("");

        // keyspace may have been created after the last refresh, its schema is described on lookup and cached
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
        verify(session, times(1)).execute("DESCRIBE KEYSPACE " + KEYSPACE.name());

        // simulate Cassandra unavailability to verify data is taken from cache
        when(sessionProvider.get()).thenThrow(new CassandraUnavailableException(CQL, "CQL unavailable"));

        // the schema of all keyspaces is assembled again, because the keyspace has been added to the cache
        assertThat(schemaCache.getSchema()).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE.name())).isEqualTo(KEYSPACE_SCHEMA);
    }

    @Test
    void testDescribedSchemaOfAllKeyspacesIsCached()
    {
        // the schema of all keyspaces is assembled during refresh, and it is served from the cache afterwards
        assertThat(schemaCache.getSchema()).isSameAs(schemaCache.getSchema());
        assertThat(schemaCache.getSchema()).isEqualTo(KEYSPACE_SCHEMA);
    }

    @Test
    void testDescribedSchemaOfAllKeyspacesIsInvalidatedOnDemandLookup()
    {
        assertThat(schemaCache.getSchema()).isEqualTo(KEYSPACE_SCHEMA);

        // a keyspace created after the last refresh is described on lookup, which invalidates the schema of
        // all keyspaces, so that the new keyspace is included when it is assembled again
        String otherSchema = "CREATE TABLE keyspace2.table1 (a int PRIMARY KEY);";
        mockDescribeKeyspace(session, "keyspace2", otherSchema);

        assertThat(schemaCache.getKeyspaceSchema("keyspace2")).isEqualTo(keyspaceSchema("keyspace2", otherSchema));
        assertThat(schemaCache.getSchema())
        .isEqualTo(KEYSPACE_SCHEMA + "\n\n" + keyspaceSchema("keyspace2", otherSchema));
    }

    @Test
    void testDescribedSchemaIsAssembledInStableOrder()
    {
        // the keyspaces are queried into a set, so the cache has to keep them sorted for the assembled schema
        // to be repeatable
        String otherSchema = "CREATE TABLE aaa_keyspace.table1 (a int PRIMARY KEY);";
        mockDescribeKeyspace(session, "aaa_keyspace", otherSchema);
        keyspaceRows.add(Map.of("keyspace_name", "aaa_keyspace"));

        assertThat(schemaCache.getSchema())
        .isEqualTo(keyspaceSchema("aaa_keyspace", otherSchema) + "\n\n" + KEYSPACE_SCHEMA);
    }

    @Test
    void testDroppedKeyspaceIsRemovedOnRefresh()
    {
        assertThat(schemaCache.getSchema()).isEqualTo(KEYSPACE_SCHEMA);

        // the keyspace is dropped, so Cassandra no longer lists it
        keyspaceRows.clear();
        schemaCache.refresh(false);

        // the cache is replaced on every refresh, therefore the dropped keyspace is gone
        assertThat(schemaCache.getSchema()).isEqualTo("");
    }

    @Test
    void testKeyspaceWithoutSchemaIsNotCached()
    {
        // Cassandra renders no statement for some keyspaces, such as the virtual ones
        keyspaceRows.add(Map.of("keyspace_name", VIRTUAL_KEYSPACE));

        assertThat(schemaCache.getSchema()).isEqualTo(KEYSPACE_SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(VIRTUAL_KEYSPACE)).isNull();
    }

    @Test
    void testLookupWhileCassandraIsUnavailable()
    {
        CQLSessionProvider unavailable = mock(CQLSessionProvider.class);
        when(unavailable.get()).thenThrow(new CassandraUnavailableException(CQL, "CQL unavailable"));
        SchemaCache cache = new SchemaCache(mockSidecarConfiguration(), unavailable);

        // the cache remains uninitialized, and the schema assembled from it is empty
        assertThat(cache.getSchema()).isEqualTo("");
        assertThat(cache.getUnsupportedTableSchema()).isEqualTo("");

        // a lookup missing from the cache queries Cassandra, so the unavailability is reported to the caller
        assertThatThrownBy(() -> cache.getKeyspaceSchema(KEYSPACE.name()))
        .isInstanceOf(CassandraUnavailableException.class);
        assertThatThrownBy(() -> cache.getUnsupportedTableSchema(KEYSPACE, TABLE))
        .isInstanceOf(CassandraUnavailableException.class);

        // unless the lookup does not allow a refresh
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
    void testDelayIsTakenFromConfiguration()
    {
        assertThat(schemaCache.delay()).isEqualTo(new SecondBoundConfiguration(5, TimeUnit.SECONDS));
    }

    @Test
    void testConcatSchemas()
    {
        assertThat(SchemaCache.concatSchemas()).isEqualTo("");
        assertThat(SchemaCache.concatSchemas("", null)).isEqualTo("");
        assertThat(SchemaCache.concatSchemas(SCHEMA)).isEqualTo(SCHEMA);
        // blank schemas are skipped, and the remaining ones are separated with an empty line
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

    /**
     * Stubs a statement Cassandra is mocked to answer with the given rows. The rows are read when the statement is
     * executed, so that the caller can modify them between two executions.
     *
     * @param session   the mocked session
     * @param statement the statement to stub
     * @param rows      the rows to return, keyed by column name
     */
    static void mockQuery(Session session, String statement, List<Map<String, String>> rows)
    {
        when(session.execute(eq(statement))).then(invocation -> mockResultSet(rows));
    }

    static void mockDescribeKeyspace(Session session, String keyspace, String schema)
    {
        // DESCRIBE KEYSPACE renders the keyspace and every object it contains, one statement per row
        mockQuery(session, "DESCRIBE KEYSPACE " + keyspace,
                  List.of(Map.of("create_statement", createKeyspaceStatement(keyspace)),
                          Map.of("create_statement", schema)));
    }

    static void mockDescribeTable(Session session, String keyspace, String table, String schema)
    {
        mockQuery(session, String.format("DESCRIBE TABLE %s.%s", keyspace, table),
                  List.of(Map.of("create_statement", schema)));
    }

    /**
     * @param keyspace the keyspace the Java driver is mocked to know
     * @param table    the single table of the keyspace the Java driver is mocked to know
     * @return the mocked driver metadata of the keyspace
     */
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
