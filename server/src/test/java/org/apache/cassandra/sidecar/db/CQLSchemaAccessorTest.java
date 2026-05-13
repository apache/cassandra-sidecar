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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.Name;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CQLSchemaAccessorTest
{
    static final Name KEYSPACE = new Name("keyspace1");
    static final Name TABLE = new Name("table1");
    static final String SCHEMA = "CREATE TABLE keyspace1.table1 (a int, b int, PRIMARY KEY (a));";

    CQLSchemaAccessor schemaAccessor;

    @BeforeEach
    void setUp()
    {
        CQLSessionProvider sessionProvider = mockCQLSessionProvider(KEYSPACE.name(), TABLE.name(), SCHEMA);
        schemaAccessor = new CQLSchemaAccessor(sessionProvider);
    }

    @Test
    void testGetKeyspaceNames()
    {
        assertThat(schemaAccessor.getKeyspaces()).containsExactly(KEYSPACE);
    }

    @Test
    void testGetExistingTable()
    {
        assertThat(schemaAccessor.getTableSchema(KEYSPACE, TABLE)).containsExactly(SCHEMA);
    }

    @Test
    void testGetNotExistingTable()
    {
        assertThat(schemaAccessor.getTableSchema(KEYSPACE, new Name("unknown"))).isNull();
    }

    static CQLSessionProvider mockCQLSessionProvider(String keyspace, String table, String schema)
    {
        Session session = mock(Session.class, RETURNS_DEEP_STUBS);

        when(session.execute(eq("DESCRIBE KEYSPACES"))).then(invocation -> {
            ResultSet resultSet = mock(ResultSet.class);
            Row row = mockRow(Map.of("keyspace_name", keyspace));
            when(resultSet.all()).thenReturn(List.of(row));
            return resultSet;
        });

        when(session.execute(startsWith("DESCRIBE TABLE"))).thenThrow(new InvalidQueryException("Unknown table"));

        String describeTable = String.format("DESCRIBE TABLE %s.%s", keyspace, table);
        when(session.execute(eq(describeTable))).then(invocation -> {
            ResultSet resultSet = mock(ResultSet.class);
            Row row = mockRow(Map.of("create_statement", schema));
            when(resultSet.all()).thenReturn(List.of(row));
            return resultSet;
        });

        CQLSessionProvider cqlSession = mock(CQLSessionProvider.class);
        when(cqlSession.get()).thenReturn(session);

        return cqlSession;
    }

    static Row mockRow(Map<String, String> values)
    {
        Row row = mock(Row.class);
        values.forEach((k, v) -> when(row.getString(k)).thenReturn(v));
        return row;
    }
}
