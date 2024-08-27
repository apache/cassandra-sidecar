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

import java.util.concurrent.CompletableFuture;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.ext.auth.authorization.AndAuthorization;
import org.apache.cassandra.sidecar.utils.CacheFactory;

/**
 * Contains the caches that hold the information necessary for authorizing a user
 */
@Singleton
public class PermissionsAccessor
{
    private AsyncLoadingCache<String, Boolean> rolesToSuperUserCache;
    private AsyncLoadingCache<String, String> identityToRolesCache;
    private AsyncLoadingCache<String, AndAuthorization> roleToPermissionsCache;

    @Inject
    public PermissionsAccessor(CacheFactory cacheFactory)
    {

        this.rolesToSuperUserCache = cacheFactory.rolesToSuperUserCache();
        this.identityToRolesCache = cacheFactory.identityToRolesCache();
        this.roleToPermissionsCache = cacheFactory.roleToPermissionsCache();
    }

    /**
     * Returns a {@code CompletableFuture} containing the role associated with the inputted
     * identity. If the identity doesn't exist in the cache, null will be returned.
     *
     * @param identity - identity to be looked up in the cache
     * @return - A {@code CompletableFuture}
     */
    public CompletableFuture<String> getRoleFromIdentity(String identity)
    {
        return identityToRolesCache.getIfPresent(identity);
    }

    /**
     * Returns a {@code CompletableFuture} that contains {@code true} if the given role
     * is a superuser and {@code false} otherwise
     *
     * @param role - the role to be looked up in the cache
     * @return - A {@code CompletableFuture}
     */
    public CompletableFuture<Boolean> getSuperUserStatus(String role)
    {
        return rolesToSuperUserCache.getIfPresent(role);
    }

    /**
     * Returns a {@code CompletableFuture} containing the {@code AndAuthorization} representing all
     * the permissions associated with the inputted role.
     *
     * @param role - the role to be looked up in the cache
     * @return - A {@code CompletableFuture}
     */
    public CompletableFuture<AndAuthorization> getPermissions(String role)
    {
        return roleToPermissionsCache.getIfPresent(role);
    }

    /**
     * @return - the cache mapping roles to superusers
     */
    public AsyncLoadingCache<String, Boolean> getRolesToSuperUserCache()
    {
        return rolesToSuperUserCache;
    }

    /**
     * @return - the cache mapping identity to roles
     */
    public AsyncLoadingCache<String, String> getIdentityToRolesCache()
    {
        return identityToRolesCache;
    }

    /**
     * @return - the cache mapping roles to permissions
     */
    public AsyncLoadingCache<String, AndAuthorization> getRoleToPermissionsCache()
    {
        return roleToPermissionsCache;
    }

    /**
     * Sets this instances caches to the caches passed in as input
     *
     * @param rolesToSuperUserCache  - cache mapping roles to superusers
     * @param identityToRolesCache   - cache mapping identity to roles
     * @param roleToPermissionsCache - cache mapping roles to permissions
     */
    public void setCaches(AsyncLoadingCache<String, Boolean> rolesToSuperUserCache,
                          AsyncLoadingCache<String, String> identityToRolesCache,
                          AsyncLoadingCache<String, AndAuthorization> roleToPermissionsCache)
    {
        this.rolesToSuperUserCache = rolesToSuperUserCache;
        this.identityToRolesCache = identityToRolesCache;
        this.roleToPermissionsCache = roleToPermissionsCache;
    }
}
