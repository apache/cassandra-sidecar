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

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.DatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;

/**
 * Database Accessor that queries cassandra to get all the necessary information
 * associated with a user to authorize them
 */
@Singleton
public class SystemAuthDatabaseAccessor extends DatabaseAccessor<SystemAuthSchema>
{
    @Inject
    public SystemAuthDatabaseAccessor(SidecarSchema sidecarSchema,
                                      SystemAuthSchema systemAuthSchema,
                                      CQLSessionProvider sessionProvider)
    {
        super(sidecarSchema, systemAuthSchema, sessionProvider);
    }

    /**
     * Queries Cassandra for the role associated with the inputted identity.
     *
     * @param identity - The identity to be queried in Cassandra
     * @return - the role associated with the given identity in Cassandra
     */
    public String findRoleFromIdentity(String identity)
    {
        BoundStatement statement = tableSchema.selectRolesFromIdentity()
                                              .bind(identity);
        ResultSet result = execute(statement);

        return result.one().getString("role");
    }

    /**
     * Queries cassandra to get the permissions associated with the
     * inputted role. These permissions are in an AndAuthorization which
     * lists every authorization that the user has where each one is connected
     * to a resource.
     *
     * @param role - the role to be queried in Cassandra
     * @return - the associated permissions
     */
    public AndAuthorization findPermissionsFromResourceRole(String role)
    {
        BoundStatement statement = tableSchema.selectPermissionsFromResourceRole()
                                              .bind(role);
        ResultSet result = execute(statement);
        AndAuthorization permissions = AndAuthorization.create();
        for (Row row : result)
        {
            for (String permission : row.getSet("permissions", String.class))
            {
                if (!permissions.verify(RoleBasedAuthorization.create(permission).setResource(row.getString("resource"))))
                {
                    permissions.addAuthorization(RoleBasedAuthorization.create(permission).setResource(row.getString("resource")));
                }
            }
        }
        return permissions;
    }

    /**
     * Queries Cassandra to see whether a role is a superuser or not.
     *
     * @param role - the role to be queried
     * @return - {@code true} if the role is a superuser and {@code false} otherwise
     */
    public boolean isSuperUser(String role)
    {
        BoundStatement statement = tableSchema.selectIsSuperUser()
                                              .bind(role);

        ResultSet result = execute(statement);
        Row userRow = result.one();
        if (userRow == null)
        {
            return false;
        }
        return userRow.getBool("is_superuser");
    }

    /**
     * Queries Cassandra for all rows in identity to role table
     *
     * @return - {@code List<Row>} containing each row in the identity to roles table
     */
    public List<Row> getAllRolesAndIdentities()
    {
        BoundStatement statement = tableSchema.getAllRolesAndIdentities().bind();

        ResultSet resultSet = execute(statement);
        List<Row> results = new ArrayList<>();
        for (Row row : resultSet)
        {
            results.add(row);
        }
        return results;
    }

    /**
     * Queries Cassandra for all rows in the resource, role to permissions table
     *
     * @return - {@code List<Row>} containing each row in the table mapping
     * resources, role to permissions
     */
    public List<Row> getAllPermissionsFromResourceRole()
    {
        BoundStatement statement = tableSchema.getAllPermissionsFromResourceRole().bind();

        ResultSet resultSet = execute(statement);
        List<Row> results = new ArrayList<>();
        for (Row row : resultSet)
        {
            results.add(row);
        }
        return results;
    }

    /**
     * Queries Cassandra for all roles in the roles table along with whether they are superusers
     *
     * @return - {@code List<Row>} of roles and their superuser status
     */
    public List<Row> getAllRoles()
    {
        BoundStatement statement = tableSchema.getAllRoles().bind();

        ResultSet resultSet = execute(statement);
        List<Row> results = new ArrayList<>();
        for (Row row : resultSet)
        {
            results.add(row);
        }
        return results;
    }
}
