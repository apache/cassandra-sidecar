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

package org.apache.cassandra.sidecar.auth.authorization;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.jetbrains.annotations.NotNull;

/**
 * Schema for getting the permissions from Cassandra through queries.
 */
public class SystemAuthSchema extends TableSchema
{
    PreparedStatement selectRolesFromIdentity;
    PreparedStatement selectPermissionsFromResourceRole;
    PreparedStatement selectIsSuperUser;
    PreparedStatement getAllRolesAndIdentities;
    PreparedStatement getAllPermissionsFromResourceRole;
    PreparedStatement getAllRoles;

    protected String keyspaceName()
    {
        return "system_auth";
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        selectRolesFromIdentity = prepare(selectRolesFromIdentity,
                                          session,
                                          CqlLiterals.selectRolesFromIdentity());
        selectPermissionsFromResourceRole = prepare(selectPermissionsFromResourceRole,
                                                    session,
                                                    CqlLiterals.selectPermissionsFromResourceRole());
        selectIsSuperUser = prepare(selectIsSuperUser,
                                    session,
                                    CqlLiterals.selectIsSuperUser());

        getAllRolesAndIdentities = prepare(getAllRolesAndIdentities,
                                           session,
                                           CqlLiterals.getAllRolesAndIdentities());

        getAllPermissionsFromResourceRole = prepare(getAllPermissionsFromResourceRole,
                                                    session,
                                                    CqlLiterals.getAllPermissionsFromResourceRole());

        getAllRoles = prepare(getAllRoles,
                              session,
                              CqlLiterals.getAllRoles());
    }

    protected String tableName()
    {
        throw new UnsupportedOperationException("The method should not be called");
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        return true;
    }

    @Override
    protected String createSchemaStatement()
    {
        return ("CREATE TABLE IF NOT EXISTS system_auth.identity_to_role (" +
                "identity text," +
                "role text)");
    }

    public PreparedStatement selectRolesFromIdentity()
    {
        return selectRolesFromIdentity;
    }

    public PreparedStatement selectPermissionsFromResourceRole()
    {
        return selectPermissionsFromResourceRole;
    }

    public PreparedStatement selectIsSuperUser()
    {
        return selectIsSuperUser;
    }

    public PreparedStatement getAllRolesAndIdentities()
    {
        return getAllRolesAndIdentities;
    }

    public PreparedStatement getAllPermissionsFromResourceRole()
    {
        return getAllPermissionsFromResourceRole;
    }

    public PreparedStatement getAllRoles()
    {
        return getAllRoles;
    }

    private static class CqlLiterals
    {
        static String selectRolesFromIdentity()
        {
            return "SELECT role FROM system_auth.identity_to_role WHERE identity = ?";
        }

        static String selectPermissionsFromResourceRole()
        {
            return "SELECT resource, permissions FROM system_auth.role_permissions WHERE role = ?";
        }

        static String selectIsSuperUser()
        {
            return "SELECT is_superuser FROM system_auth.roles WHERE role = ?;";
        }

        static String getAllRolesAndIdentities()
        {
            return "SELECT * FROM system_auth.identity_to_role;";
        }

        static String getAllPermissionsFromResourceRole()
        {
            return "SELECT * FROM system_auth.role_permissions;";
        }

        static String getAllRoles()
        {
            return "SELECT role, is_superuser FROM system_auth.roles;";
        }
    }
}
