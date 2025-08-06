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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.ext.auth.authorization.Authorization;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactory;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Database Accessor that queries cassandra to get information maintained under system_auth keyspace.
 */
@Singleton
public class SystemAuthDatabaseAccessor extends DatabaseAccessor<SystemAuthSchema>
{
    private final PermissionFactory permissionFactory;

    @Inject
    public SystemAuthDatabaseAccessor(SidecarSchema sidecarSchema,
                                      CQLSessionProvider sessionProvider,
                                      PermissionFactory permissionFactory)
    {
        this(sidecarSchema.tableSchema(SystemAuthSchema.class), sessionProvider, permissionFactory);
    }

    @VisibleForTesting
    public SystemAuthDatabaseAccessor(SystemAuthSchema systemAuthSchema,
                                      CQLSessionProvider sessionProvider,
                                      PermissionFactory permissionFactory)
    {
        super(systemAuthSchema, sessionProvider);
        this.permissionFactory = permissionFactory;
    }

    /**
     * Queries Cassandra for the role associated with given identity.
     *
     * @param identity identity of user extracted
     * @return the role associated with the given identity in Cassandra
     */
    public String findRoleFromIdentity(String identity)
    {
        BoundStatement statement = tableSchema.roleFromIdentity().bind(identity);
        ResultSet result = execute(statement);
        Row row = result.one();
        return row != null ? row.getString("role") : null;
    }

    /**
     * Queries Cassandra for all rows in identity_to_role table
     *
     * @return - {@code List<Row>} containing each row in the identity to roles table
     */
    public Map<String, String> findAllIdentityToRoles()
    {
        BoundStatement statement = tableSchema.allRolesAndIdentities().bind();
        ResultSet resultSet = execute(statement);
        Map<String, String> results = new HashMap<>();
        for (Row row : resultSet)
        {
            results.put(row.getString("identity"), row.getString("role"));
        }
        return results;
    }

    /**
     * Queries Cassandra for all rows in system_auth.role_permissions table. Maps permissions of a role into
     * {@link Authorization} and returns a {@code Map} of cassandra role to authorizations
     *
     * @return - {@code Map} contains role and granted authorizations
     */
    public Map<String, Set<Authorization>> findAllRolesAndPermissions()
    {
        BoundStatement statement = tableSchema.allRolesAndPermissions().bind();
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
                    logger.error("Error parsing Cassandra permission={} resource={} role={}",
                                 permission, resource, role, e);
                }
            }
            roleAuthorizations.computeIfAbsent(role, k -> new HashSet<>()).addAll(authorizations);
        }
        return roleAuthorizations;
    }

    /**
     * Queries Cassandra for superuser status of a given role.
     *
     * @param role role in Cassandra
     * @return {@code true} if the supplied role or any other role granted to it (directly or indirectly) has superuser
     * status., {@code false} otherwise
     * Note: {@code false} response does not indicate whether the role exists or not
     */
    public boolean isSuperUser(String role)
    {
        Statement statement = new SimpleStatement(String.format(tableSchema.unpreparedListRoles(), role));
        ResultSet result = execute(statement);
        for (Row row : result)
        {
            if (row.getBool("super"))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * @return a map of roles to superuser status
     */
    public Map<String, Boolean> findAllRolesToSuperuserStatus()
    {
        BoundStatement statement = tableSchema.allRoles().bind();
        ResultSet result = execute(statement);
        List<Row> rows = result.all();
        Map<String, Boolean> roleToSuperUser = rows.stream()
                                                   .collect(Collectors.toMap(row -> row.getString("role"),
                                                                             row -> row.getBool("is_superuser")));
        for (Row row : rows)
        {
            if (roleToSuperUser.get(row.getString("role")))
            {
                // already has superuser status, skip
                continue;
            }

            // check if superuser status has been granted indirectly to this user
            Set<String> memberOf = row.getSet("member_of", String.class);
            if (memberOf != null && !memberOf.isEmpty() && memberOf.stream().anyMatch(roleToSuperUser::get))
            {
                roleToSuperUser.put(row.getString("role"), true);
            }
        }
        return roleToSuperUser;
    }
}
