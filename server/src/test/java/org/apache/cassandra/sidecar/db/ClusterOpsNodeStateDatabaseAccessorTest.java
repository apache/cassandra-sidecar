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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ClusterOpsNodeStateSchema;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ClusterOpsNodeStateDatabaseAccessor}
 */
class ClusterOpsNodeStateDatabaseAccessorTest
{
    private ClusterOpsNodeStateSchema schema;
    private Session session;
    private ClusterOpsNodeStateDatabaseAccessor accessor;

    @BeforeEach
    void setup()
    {
        schema = mock(ClusterOpsNodeStateSchema.class);
        session = mock(Session.class);
        CQLSessionProvider sessionProvider = mock(CQLSessionProvider.class);
        when(sessionProvider.get()).thenReturn(session);
        accessor = new ClusterOpsNodeStateDatabaseAccessor(schema, sessionProvider);
    }

    @Test
    void testUpdateNodeStatusesUsesUnloggedBatch()
    {
        PreparedStatement stmt = mock(PreparedStatement.class);
        BoundStatement boundStmt = mock(BoundStatement.class);
        when(schema.insertNodeStatus()).thenReturn(stmt);
        when(stmt.bind(any())).thenReturn(boundStmt);
        when(session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

        UUID operationId = UUID.randomUUID();
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        UUID nodeId3 = UUID.randomUUID();

        accessor.updateNodeStatuses("test-cluster", operationId,
                                    Arrays.asList(nodeId1, nodeId2, nodeId3),
                                    OperationalJobStatus.CREATED);

        ArgumentCaptor<Statement> captor = ArgumentCaptor.forClass(Statement.class);
        verify(session).execute(captor.capture());

        Statement executed = captor.getValue();
        assertThat(executed).isInstanceOf(BatchStatement.class);
        BatchStatement batch = (BatchStatement) executed;
        assertThat(batch.getStatements()).hasSize(3);
    }

    @Test
    void testUpdateNodeStatusesChunksLargeBatches()
    {
        PreparedStatement stmt = mock(PreparedStatement.class);
        BoundStatement boundStmt = mock(BoundStatement.class);
        when(schema.insertNodeStatus()).thenReturn(stmt);
        when(stmt.bind(any())).thenReturn(boundStmt);
        when(session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

        List<UUID> nodeIds = new ArrayList<>();
        for (int i = 0; i < 250; i++)
        {
            nodeIds.add(UUID.randomUUID());
        }

        accessor.updateNodeStatuses("test-cluster", UUID.randomUUID(), nodeIds, OperationalJobStatus.CREATED);

        ArgumentCaptor<Statement> captor = ArgumentCaptor.forClass(Statement.class);
        verify(session, times(3)).execute(captor.capture());

        List<Statement> executed = captor.getAllValues();
        assertThat(executed).hasSize(3);
        assertThat(((BatchStatement) executed.get(0)).getStatements()).hasSize(100);
        assertThat(((BatchStatement) executed.get(1)).getStatements()).hasSize(100);
        assertThat(((BatchStatement) executed.get(2)).getStatements()).hasSize(50);
    }
}
