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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;

import static org.apache.cassandra.sidecar.db.CQLSchemaAccessorTest.mockRow;
import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DriverUnsupportedSchemaCacheTest
{
    static final Name KEYSPACE = new Name("keyspace1");
    static final Name TABLE = new Name("table1");
    static final String SCHEMA = "CREATE TABLE keyspace1.table1 (a int, b vector<float, 3>, PRIMARY KEY (a));";

    CQLSessionProvider sessionProvider;
    Session session;
    DriverUnsupportedSchemaCache schemaCache;

    @BeforeEach
    void setUp()
    {
        sessionProvider = CQLSchemaAccessorTest.mockCQLSessionProvider(KEYSPACE.name(), TABLE.name(), SCHEMA);
        session = sessionProvider.get();
        mockPreparedQuery(session,
                          "SELECT keyspace_name, table_name FROM system_schema.tables",
                          List.of(Map.of("keyspace_name", KEYSPACE.name(), "table_name", TABLE.name())));
        SidecarConfiguration sidecarConfiguration = mockSidecarConfiguration();
        schemaCache = new DriverUnsupportedSchemaCache(sidecarConfiguration, sessionProvider);
    }

    @Test
    void testImmediateSchemaLookup()
    {
        assertThat(schemaCache.getFullSchema()).isEqualTo(SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE)).isEqualTo(SCHEMA);
        assertThat(schemaCache.getTableSchema(KEYSPACE, TABLE)).isEqualTo(SCHEMA);

        assertThat(schemaCache.getTableSchema(new Name("unknown"), TABLE)).isNull();
        assertThat(schemaCache.getTableSchema(KEYSPACE, new Name("unknown"))).isNull();
    }

    @Test
    void testSchemaLookupAfterRefresh()
    {
        // mark cache as initialized, but effectively empty
        schemaCache.setInitialized(true);

        assertThat(schemaCache.getFullSchema()).isEqualTo("");
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE)).isEqualTo("");

        Promise<Void> p = Promise.promise();
        schemaCache.execute(p);
        Future<Void> future = p.future();
        assertThat(future.succeeded()).isTrue();

        // simulate Cassandra unavailability to verify data is taken from cache
        when(sessionProvider.get()).thenThrow(new CassandraUnavailableException(CQL, "CQL unavailable"));

        assertThat(schemaCache.getFullSchema()).isEqualTo(SCHEMA);
        assertThat(schemaCache.getKeyspaceSchema(KEYSPACE)).isEqualTo(SCHEMA);
        assertThat(schemaCache.getTableSchema(KEYSPACE, TABLE)).isEqualTo(SCHEMA);
    }

    static SidecarConfiguration mockSidecarConfiguration()
    {
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class);
        DriverConfiguration driverConfiguration = mock(DriverConfiguration.class);
        when(driverConfiguration.unsupportedTableSchemaRefreshTime()).thenReturn(new SecondBoundConfiguration(5, TimeUnit.SECONDS));
        when(sidecarConfiguration.driverConfiguration()).thenReturn(driverConfiguration);
        return sidecarConfiguration;
    }

    static void mockPreparedQuery(Session session, String statement, List<Map<String, String>> rows)
    {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(eq(statement))).thenReturn(preparedStatement);
        when(preparedStatement.bind()).thenReturn(boundStatement);
        when(session.execute(eq(boundStatement))).then(invocation -> {
            ResultSet resultSet = mock(ResultSet.class);
            List<Row> mockRows = new ArrayList<>();
            rows.forEach(row -> mockRows.add(mockRow(row)));
            when(resultSet.all()).thenReturn(mockRows);
            return resultSet;
        });
    }
}
