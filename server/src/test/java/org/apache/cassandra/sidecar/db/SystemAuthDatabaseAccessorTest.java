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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.acl.authorization.PermissionFactoryImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

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
}
