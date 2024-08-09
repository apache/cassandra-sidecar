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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;

/**
 * {@inheritDoc}
 */
public class MutualTlsAuthorizationProvider implements AuthorizationProvider
{
    private final PermissionsAccessor permissionsAccessor;

    public MutualTlsAuthorizationProvider(PermissionsAccessor permissionsAccessor)
    {
        this.permissionsAccessor = permissionsAccessor;
    }

    /**
     * returns the id of the authorization provider
     *
     * @return
     */
    public String getId()
    {
        return "mtls";
    }

    /**
     * Updates the user with the set of authorizations.
     *
     * @param user    user to lookup and update
     * @param handler result handler
     */
    public void getAuthorizations(User user, Handler<AsyncResult<Void>> handler)
    {
        getAuthorizations(user).onComplete(handler);
    }

    public Future<Void> getAuthorizations(User user)
    {
        try
        {
            if (user == null)
            {
                return Future.failedFuture("No user provided");
            }

            String identity = user.subject();

            if (identity == null || identity.isEmpty())
            {
                return Future.failedFuture("Identity cannot be null or empty");
            }
            CompletableFuture<String> rolesFuture = permissionsAccessor.getRoleFromIdentity(identity);
            String roles = rolesFuture.get();
            if (roles == null || roles.isEmpty())
            {
                return Future.failedFuture("No roles match the given identity");
            }

            CompletableFuture<Boolean> superUserStatusFuture = permissionsAccessor.getSuperUserStatus(roles);
            Boolean superUserStatus = superUserStatusFuture.get();
            if (superUserStatus)
            {
                // Add superuser authorization to user
                Set<Authorization> superUserAuthorization = new HashSet<>();
                AndAuthorization superUserPermissions = AndAuthorization.create();
                for (MutualTlsPermissions perm : MutualTlsPermissions.ALL)
                {
                    superUserPermissions.addAuthorization(RoleBasedAuthorization.create(perm.name()).setResource("ALL KEYSPACES"));
                }
                superUserAuthorization.add(superUserPermissions);
                user.authorizations().add(getId(), superUserAuthorization);
                return Future.succeededFuture();
            }
            CompletableFuture<AndAuthorization> permissionsFuture = permissionsAccessor.getPermissions(roles);
            AndAuthorization permissions = permissionsFuture.get();
            if (permissions == null)
            {
                return Future.failedFuture("Could not get permissions for the specified identity");
            }
            user.authorizations().add(getId(), permissions);

            return Future.succeededFuture();
        }
        catch (InterruptedException | ExecutionException e)
        {
            return Future.failedFuture("Could not get permissions for the user");
        }
    }
}
