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

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactoryImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SidecarRolePermissionsSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SidecarPermissionsDatabaseAccessor}
 */
class BasicPermissionsDatabaseAccessorTest
{
    @Test
    void testReadingInvalidActions()
    {
        SidecarRolePermissionsSchema mockSchema = mock(SidecarRolePermissionsSchema.class);
        PreparedStatement mockStmt = mock(PreparedStatement.class);
        BoundStatement mockBoundStmt = mock(BoundStatement.class);
        when(mockStmt.bind()).thenReturn(mockBoundStmt);
        when(mockSchema.allRolesPermissions()).thenReturn(mockStmt);

        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        TestSidecarPermissionsDatabaseAccessor sidecarPermissionsDatabaseAccessor
        = new TestSidecarPermissionsDatabaseAccessor(mockSchema, mockSessionProvider);

        assertThat(sidecarPermissionsDatabaseAccessor.rolesToAuthorizations()).isEmpty();
    }

    static class TestSidecarPermissionsDatabaseAccessor extends SidecarPermissionsDatabaseAccessor
    {

        protected TestSidecarPermissionsDatabaseAccessor(SidecarRolePermissionsSchema tableSchema,
                                                         CQLSessionProvider sessionProvider)
        {
            super(tableSchema, sessionProvider, new PermissionFactoryImpl());
        }

        @Override
        protected ResultSet execute(Statement statement)
        {
            ResultSet mockResultSet = mock(ResultSet.class);
            Row mockRow = mock(Row.class);
            when(mockRow.getString("role")).thenReturn("test_role");
            when(mockRow.getString("resource")).thenReturn("test_resource");
            // invalid wildcard permission set
            when(mockRow.getSet("permissions", String.class)).thenReturn(Collections.singleton(":"));
            when(mockResultSet.iterator()).thenReturn(Collections.singletonList(mockRow).iterator());
            return mockResultSet;
        }
    }
}
