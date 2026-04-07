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

import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        CqlSession mockSession = mock(CqlSession.class);
        Metadata mockMetadata = mock(Metadata.class);
        KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
        when(mockMetadata.getKeyspace("system_auth")).thenReturn(Optional.of(mockKeyspaceMetadata));
        when(mockKeyspaceMetadata.getTable("identity_to_role")).thenReturn(null);
        when(mockSession.getMetadata()).thenReturn(mockMetadata);

        SystemAuthSchema systemAuthSchema = new SystemAuthSchema();
        assertThatThrownBy(systemAuthSchema::roleFromIdentity)
        .isExactlyInstanceOf(SchemaUnavailableException.class)
        .hasMessage("Table system_auth.identity_to_role does not exist");
        assertThatThrownBy(systemAuthSchema::allRolesAndIdentities)
        .isExactlyInstanceOf(SchemaUnavailableException.class)
        .hasMessage("Table system_auth.identity_to_role does not exist");
    }
}
