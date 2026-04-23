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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ClusterOpsSchema;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ClusterOpsDatabaseAccessor}
 */
class ClusterOpsDatabaseAccessorTest
{
    private ClusterOpsSchema schema;
    private Session session;
    private ClusterOpsDatabaseAccessor accessor;

    @BeforeEach
    void setup()
    {
        schema = mock(ClusterOpsSchema.class);
        session = mock(Session.class);
        CQLSessionProvider sessionProvider = mock(CQLSessionProvider.class);
        when(sessionProvider.get()).thenReturn(session);
        accessor = new ClusterOpsDatabaseAccessor(schema, sessionProvider);
    }

    @Test
    void testFindJobReturnsNullWhenNotFound()
    {
        PreparedStatement selectStmt = mock(PreparedStatement.class);
        BoundStatement boundStmt = mock(BoundStatement.class);
        when(schema.selectJob()).thenReturn(selectStmt);
        when(selectStmt.bind(any())).thenReturn(boundStmt);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenReturn(null);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        OperationalJobRecord result = accessor.findJob("test-cluster", UUIDs.timeBased());

        assertThat(result).isNull();
    }
}
