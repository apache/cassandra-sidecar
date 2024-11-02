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

package org.apache.cassandra.sidecar.db.schema;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.jetbrains.annotations.NotNull;

/**
 * Schema for getting information stored in system_auth keyspace.
 */
public class SystemAuthSchema extends TableSchema
{
    private static final String IDENTITY_TO_ROLE_TABLE = "identity_to_role";
    PreparedStatement selectRoleFromIdentity;
    PreparedStatement getAllRolesAndIdentities;

    protected String keyspaceName()
    {
        return "system_auth";
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        selectRoleFromIdentity = prepare(selectRoleFromIdentity,
                                          session,
                                          CqlLiterals.selectRoleFromIdentity());

        getAllRolesAndIdentities = prepare(getAllRolesAndIdentities,
                                           session,
                                           CqlLiterals.getAllRolesAndIdentities());
    }

    protected String tableName()
    {
        throw new UnsupportedOperationException("SystemAuthSchema supports reading information from multiple " +
                                                "tables in system_auth keyspace");
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        // check tables exists before preparing
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspaceName());
        // identity_to_role table exists in Cassandra versions starting 5.x
        return keyspaceMetadata != null && keyspaceMetadata.getTable(IDENTITY_TO_ROLE_TABLE) != null;
    }

    @Override
    protected String createSchemaStatement()
    {
        return "";
    }

    @Override
    protected boolean initializeInternal(@NotNull Session session)
    {
        // initialize schema after we make sure the necessary tables are present
        if (exists(session.getCluster().getMetadata()))
        {
            return super.initializeInternal(session);
        }
        return true;
    }

    public PreparedStatement selectRoleFromIdentity()
    {
        return selectRoleFromIdentity;
    }

    public PreparedStatement getAllRolesAndIdentities()
    {
        return getAllRolesAndIdentities;
    }

    private static class CqlLiterals
    {
        static String selectRoleFromIdentity()
        {
            return "SELECT role FROM system_auth.identity_to_role WHERE identity = ?";
        }

        static String getAllRolesAndIdentities()
        {
            return "SELECT * FROM system_auth.identity_to_role;";
        }
    }
}
