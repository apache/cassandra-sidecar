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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.ext.auth.authorization.Authorization;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactory;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SidecarRolePermissionsSchema;

/**
 * {@link SidecarPermissionsDatabaseAccessor} is an accessor for role_permissions_v1 table under sidecar_internal
 * keyspace. Custom sidecar specific permissions are stored in this table.
 */
@Singleton
public class SidecarPermissionsDatabaseAccessor extends DatabaseAccessor<SidecarRolePermissionsSchema>
{
    private final PermissionFactory permissionFactory;

    @Inject
    protected SidecarPermissionsDatabaseAccessor(SidecarRolePermissionsSchema tableSchema,
                                                 CQLSessionProvider sessionProvider,
                                                 PermissionFactory permissionFactory)
    {
        super(tableSchema, sessionProvider);
        this.permissionFactory = permissionFactory;
    }

    /**
     * Queries Sidecar for all rows in sidecar_internal.role_permissions_v1 table. This table maps permissions of a
     * role into {@link Authorization} and returns a {@code Map} of user role to authorizations.
     *
     * @return {@code Map} contains role and granted authorizations
     */
    public Map<String, Set<Authorization>> rolesToAuthorizations()
    {
        BoundStatement statement = tableSchema.allRolesPermissions().bind();
        ResultSet result = execute(statement);
        Map<String, Set<Authorization>> roleAuthorizations = new HashMap<>();
        for (Row row : result)
        {
            String role = row.getString("role");
            String resource = row.getString("resource");
            Set<String> permissions = row.getSet("permissions", String.class);
            Set<Authorization> authorizations = new HashSet<>();
            for (String permission : permissions)
            {
                try
                {
                    authorizations.add(permissionFactory.createPermission(permission).toAuthorization(resource));
                }
                catch (Exception e)
                {
                    logger.error("Error parsing Sidecar permission={} resource={} role={}",
                                 permission, resource, role, e);
                }
            }
            if (!authorizations.isEmpty())
            {
                roleAuthorizations.computeIfAbsent(role, k -> new HashSet<>()).addAll(authorizations);
            }
        }
        return roleAuthorizations;
    }
}
