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

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ActiveClusterOpsSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ActiveClusterOpsDatabaseAccessor}
 */
class ActiveClusterOpsDatabaseAccessorTest
{
    private static final String CLUSTER_NAME = "test-cluster";

    private ActiveClusterOpsSchema schema;
    private Session session;
    private ActiveClusterOpsDatabaseAccessor accessor;

    @BeforeEach
    void setup()
    {
        schema = mock(ActiveClusterOpsSchema.class);
        session = mock(Session.class);
        CQLSessionProvider sessionProvider = mock(CQLSessionProvider.class);
        when(sessionProvider.get()).thenReturn(session);
        accessor = new ActiveClusterOpsDatabaseAccessor(schema, sessionProvider);
    }

    @Test
    void testTrySetActiveOperationSucceeds()
    {
        PreparedStatement stmt = mock(PreparedStatement.class);
        BoundStatement boundStmt = mock(BoundStatement.class);
        when(schema.trySetActive()).thenReturn(stmt);
        when(stmt.bind(any())).thenReturn(boundStmt);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.wasApplied()).thenReturn(true);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        boolean result = accessor.trySetActiveOperation(CLUSTER_NAME, "restart", UUID.randomUUID());

        assertThat(result).isTrue();
    }

    @Test
    void testTrySetActiveOperationFailsWhenAlreadyActive()
    {
        PreparedStatement stmt = mock(PreparedStatement.class);
        BoundStatement boundStmt = mock(BoundStatement.class);
        when(schema.trySetActive()).thenReturn(stmt);
        when(stmt.bind(any())).thenReturn(boundStmt);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.wasApplied()).thenReturn(false);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        boolean result = accessor.trySetActiveOperation(CLUSTER_NAME, "restart", UUID.randomUUID());

        assertThat(result).isFalse();
    }

    @Test
    void testClearActiveOperationSucceeds()
    {
        PreparedStatement stmt = mock(PreparedStatement.class);
        BoundStatement boundStmt = mock(BoundStatement.class);
        when(schema.clearActive()).thenReturn(stmt);
        when(stmt.bind(any())).thenReturn(boundStmt);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.wasApplied()).thenReturn(true);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        boolean result = accessor.clearActiveOperation(CLUSTER_NAME, "restart", UUID.randomUUID());

        assertThat(result).isTrue();
    }

    @Test
    void testClearActiveOperationFailsWhenIdMismatch()
    {
        PreparedStatement stmt = mock(PreparedStatement.class);
        BoundStatement boundStmt = mock(BoundStatement.class);
        when(schema.clearActive()).thenReturn(stmt);
        when(stmt.bind(any())).thenReturn(boundStmt);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.wasApplied()).thenReturn(false);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        boolean result = accessor.clearActiveOperation(CLUSTER_NAME, "restart", UUID.randomUUID());

        assertThat(result).isFalse();
    }
}
