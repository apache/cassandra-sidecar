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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;
import org.jetbrains.annotations.NotNull;

/**
 * Schema for getting information stored in system_auth keyspace.
 */
@Singleton
public class SystemAuthSchema extends CassandraSystemTableSchema
{
    private static final String IDENTITY_TO_ROLE_TABLE = "identity_to_role";

    private PreparedStatement roleFromIdentity;
    private PreparedStatement allRolesAndIdentities;
    private PreparedStatement roleSuperuserStatus;
    private PreparedStatement allRoles;
    private PreparedStatement allRolesAndPermissions;

    @Override
    protected String keyspaceName()
    {
        return "system_auth";
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        roleSuperuserStatus = prepare(roleSuperuserStatus, session, "SELECT is_superuser FROM system_auth.roles WHERE role = ?");
        allRoles = prepare(allRoles, session, "SELECT * FROM system_auth.roles");
        allRolesAndPermissions = prepare(allRolesAndPermissions, session, "SELECT * FROM system_auth.role_permissions");

        KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keyspaceName());
        // identity_to_role table exists in Cassandra versions starting 5.x
        if (keyspaceMetadata == null || keyspaceMetadata.getTable(IDENTITY_TO_ROLE_TABLE) == null)
        {
            logger.info("system_auth.identity_to_role does not exist. Skip preparing. table={}.{}", keyspaceName(), IDENTITY_TO_ROLE_TABLE);
            return;
        }
        roleFromIdentity = prepare(roleFromIdentity, session, "SELECT role FROM system_auth.identity_to_role WHERE identity = ?");
        allRolesAndIdentities = prepare(allRolesAndIdentities, session, "SELECT role, identity FROM system_auth.identity_to_role");
    }

    @Override
    protected String tableName()
    {
        throw new UnsupportedOperationException("SystemAuthSchema supports reading information from multiple " +
                                                "tables in system_auth keyspace");
    }

    @NotNull
    public PreparedStatement roleFromIdentity()
    {
        ensureSchemaAvailable();
        return roleFromIdentity;
    }

    @NotNull
    public PreparedStatement allRolesAndIdentities()
    {
        ensureSchemaAvailable();
        return allRolesAndIdentities;
    }

    public PreparedStatement allRolesAndPermissions()
    {
        return allRolesAndPermissions;
    }

    public PreparedStatement roleSuperuserStatus()
    {
        return roleSuperuserStatus;
    }

    public PreparedStatement allRoles()
    {
        return allRoles;
    }

    protected void ensureSchemaAvailable() throws SchemaUnavailableException
    {
        if (roleFromIdentity == null || allRolesAndIdentities == null)
        {
            throw new SchemaUnavailableException(keyspaceName(), IDENTITY_TO_ROLE_TABLE);
        }
    }
}
