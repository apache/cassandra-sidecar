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
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactoryImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SystemAuthDatabaseAccessor}
 */
class SystemAuthDatabaseAccessorTest
{
    @Test
    void testAccessorThrowsWhenTableNotFound()
    {
        SystemAuthSchema systemAuthSchema = new SystemAuthSchema();
        CQLSessionProvider mockCqlSessionProvider = mock(CQLSessionProvider.class);
        SystemAuthDatabaseAccessor systemAuthDatabaseAccessor = new SystemAuthDatabaseAccessor(systemAuthSchema,
                                                                                               mockCqlSessionProvider,
                                                                                               new PermissionFactoryImpl());
        assertThatThrownBy(()  -> systemAuthDatabaseAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test"))
        .isInstanceOf(SchemaUnavailableException.class)
        .hasMessage("Table system_auth.identity_to_role does not exist");
        assertThatThrownBy(systemAuthDatabaseAccessor::findAllIdentityToRoles)
        .isInstanceOf(SchemaUnavailableException.class)
        .hasMessage("Table system_auth.identity_to_role does not exist");
    }

    @Test
    void testReadingMemberOf()
    {
        SystemAuthSchema mockSystemAuthSchema = mock(SystemAuthSchema.class);
        CQLSessionProvider mockCqlSessionProvider = mock(CQLSessionProvider.class);
        Session mockSession = mock(Session.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockSystemAuthSchema.allRoles()).thenReturn(mockPreparedStatement);
        when(mockCqlSessionProvider.get()).thenReturn(mockSession);
        ResultSet mockResult = mock(ResultSet.class);
        List<Row> rows = new ArrayList<>();
        Row mockRow1 = mock(Row.class);
        when(mockRow1.getString("role")).thenReturn("super_role");
        when(mockRow1.getBool("is_superuser")).thenReturn(true);
        when(mockRow1.getSet("member_of", String.class)).thenReturn(Set.of());
        Row mockRow2 = mock(Row.class);
        when(mockRow2.getString("role")).thenReturn("non_super_role");
        when(mockRow2.getBool("is_superuser")).thenReturn(false);
        when(mockRow2.getSet("member_of", String.class)).thenReturn(Set.of("super_role"));
        rows.add(mockRow1);
        rows.add(mockRow2);
        when(mockResult.all()).thenReturn(rows);
        when(mockSession.execute((Statement) any())).thenReturn(mockResult);
        SystemAuthDatabaseAccessor systemAuthDatabaseAccessor = new SystemAuthDatabaseAccessor(mockSystemAuthSchema,
                                                                                               mockCqlSessionProvider,
                                                                                               new PermissionFactoryImpl());
        Map<String, Boolean> superUserCache = systemAuthDatabaseAccessor.findAllRolesToSuperuserStatus();
        assertThat(superUserCache.size()).isEqualTo(2);
        assertThat(superUserCache.get("super_role")).isTrue();
        assertThat(superUserCache.get("non_super_role")).isTrue();
    }
}
