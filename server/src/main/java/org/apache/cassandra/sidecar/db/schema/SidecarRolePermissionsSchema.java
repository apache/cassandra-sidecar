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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * sidecar_internal.role_permissions_v1 table holds custom sidecar permissions that are not stored in Cassandra.
 * Permissions are stored against resource.
 */
@Singleton
public class SidecarRolePermissionsSchema extends TableSchema
{
    private static final String ROLE_PERMISSIONS_TABLE = "role_permissions_v1";

    private final SchemaKeyspaceConfiguration keyspaceConfig;

    private PreparedStatement allRolesAndPermissions;

    @Inject
    public SidecarRolePermissionsSchema(SidecarConfiguration sidecarConfiguration)
    {
        this.keyspaceConfig = sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration();
    }

    @Override
    protected String tableName()
    {
        return ROLE_PERMISSIONS_TABLE;
    }

    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        allRolesAndPermissions = prepare(allRolesAndPermissions, session, CqlLiterals.allRolesAndPermissions(keyspaceConfig));
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s ("
                             + "role text,"
                             + "resource text,"
                             + "permissions set<text>,"
                             + "PRIMARY KEY(role, resource))",
                             keyspaceConfig.keyspace(), ROLE_PERMISSIONS_TABLE);
    }

    public PreparedStatement allRolesPermissions()
    {
        return allRolesAndPermissions;
    }

    private static class CqlLiterals
    {
        static String allRolesAndPermissions(SchemaKeyspaceConfiguration config)
        {
            return String.format("SELECT * FROM %s.%s", config.keyspace(), ROLE_PERMISSIONS_TABLE);
        }
    }
}
