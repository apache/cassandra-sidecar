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

package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.authorization.Authorization;
import org.apache.cassandra.sidecar.acl.AuthCache;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SidecarPermissionsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;

/**
 * Caches role and authorizations held by it. Entries from system_auth.role_permissions table in Cassandra and
 * sidecar_internal.role_permissions_v1 table are processed into authorizations and cached here. All table entries are
 * stored against a unique cache key. Caching against UNIQUE_CACHE_ENTRY is done to make sure new entries in the table
 * are picked up during cache refreshes.
 */
@Singleton
public class RoleAuthorizationsCache extends AuthCache<String, Map<String, Set<Authorization>>>
{
    private static final String NAME = "role_permissions_cache";
    protected static final String UNIQUE_CACHE_ENTRY = "unique_cache_entry_key";

    @Inject
    public RoleAuthorizationsCache(Vertx vertx,
                                   ExecutorPools executorPools,
                                   SidecarConfiguration sidecarConfiguration,
                                   SidecarSchema sidecarSchema,
                                   SystemAuthDatabaseAccessor systemAuthDatabaseAccessor,
                                   SidecarPermissionsDatabaseAccessor sidecarPermissionsDatabaseAccessor)
    {
        super(NAME,
              vertx,
              executorPools,
              k -> loadAuthorizations(systemAuthDatabaseAccessor,
                                      sidecarSchema,
                                      sidecarPermissionsDatabaseAccessor),
              () -> Collections.singletonMap(UNIQUE_CACHE_ENTRY,
                                             loadAuthorizations(systemAuthDatabaseAccessor,
                                                                sidecarSchema,
                                                                sidecarPermissionsDatabaseAccessor)),
              sidecarConfiguration.accessControlConfiguration().permissionCacheConfiguration());
    }

    /**
     * {@code get} retrieves all role to authorizations mapping maintained in this cache.
     *
     * @param ignored key for retrieval. Cache always contains one entry stored against unique_cache_entry_key,
     *                hence key is ignored
     * @return Cache entry stored against unique_cache_entry_key
     */
    @Override
    public Map<String, Set<Authorization>> get(String ignored)
    {
        // RoleAuthorizationsCache stores all entries of role to authorizations mappings against a single key,
        // which is unique_cache_entry_key. This is to refresh and pick up all entries and their changes from
        // Cassandra's role_permissions table and Sidecar role_permissions_v1 table. Hence during cache retrieval
        // key is ignored and entry stored against unique_cache_entry_key is returned
        return super.get(UNIQUE_CACHE_ENTRY);
    }

    /**
     * Provides authorizations given user's role.
     *
     * @param role a role a user holds
     * @return a {@code Set} of {@link Authorization} a role holds.
     */
    public Set<Authorization> getAuthorizations(String role)
    {
        Map<String, Set<Authorization>> roleAuthorizations = get(UNIQUE_CACHE_ENTRY);
        return roleAuthorizations != null ? roleAuthorizations.get(role) : Collections.emptySet();
    }

    private static Map<String, Set<Authorization>> loadAuthorizations(SystemAuthDatabaseAccessor systemAuthDatabaseAccessor,
                                                                      SidecarSchema sidecarSchema,
                                                                      SidecarPermissionsDatabaseAccessor sidecarPermissionsDatabaseAccessor)
    {
        Map<String, Set<Authorization>> roleAuthorizations = systemAuthDatabaseAccessor.findAllRolesAndPermissions();

        if (sidecarSchema.isInitialized())
        {
            Map<String, Set<Authorization>> sidecarAuthorizations
            = sidecarPermissionsDatabaseAccessor.rolesToAuthorizations();

            // merge authorizations from Cassandra and Sidecar tables
            sidecarAuthorizations.forEach((role, authorizations) -> {
                roleAuthorizations.merge(role, authorizations, (existingAuthorizations, newAuthorizations) -> {
                    existingAuthorizations.addAll(newAuthorizations);
                    return existingAuthorizations;
                });
            });
        }
        return roleAuthorizations;
    }
}
