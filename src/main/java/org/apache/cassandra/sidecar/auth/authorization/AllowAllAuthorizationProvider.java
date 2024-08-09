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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;

/**
 * Authorizer to allow all requests and grant every request all permissions
 */
public class AllowAllAuthorizationProvider implements AuthorizationProvider
{
    public String getId()
    {
        return "AllowAll";
    }

    public void getAuthorizations(User user, Handler<AsyncResult<Void>> handler)
    {
        getAuthorizations(user).onComplete(handler);
    }

    public Future<Void> getAuthorizations(User user)
    {
        if (user == null)
        {
            return Future.failedFuture("User cannot be null");
        }

        Set<Authorization> allowAllAuthorizations = new HashSet<>();
        AndAuthorization allowAllPermissions = AndAuthorization.create();
        for (MutualTlsPermissions perm : MutualTlsPermissions.ALL)
        {
            allowAllPermissions.addAuthorization(RoleBasedAuthorization.create(perm.name()).setResource("ALL KEYSPACES"));
        }
        allowAllAuthorizations.add(allowAllPermissions);
        user.authorizations().add(getId(), allowAllAuthorizations);

        return Future.succeededFuture();
    }
}
