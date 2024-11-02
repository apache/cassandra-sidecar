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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link org.apache.cassandra.sidecar.db.schema.SystemAuthSchema}
 */
class SystemAuthSchemaTest
{
    @Test
    void testSchemaNotPreparedWhenTableNotFound()
    {
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
        when(mockMetadata.getKeyspace("system_auth")).thenReturn(mockKeyspaceMetadata);
        when(mockKeyspaceMetadata.getTable("identity_to_role")).thenReturn(null);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        when(mockSession.getCluster()).thenReturn(mockCluster);

        SystemAuthSchema systemAuthSchema = new SystemAuthSchema();
        systemAuthSchema.initialize(mockSession);
        assertThat(systemAuthSchema.selectRoleFromIdentity()).isNull();
        assertThat(systemAuthSchema.getAllRolesAndIdentities()).isNull();
    }
}
