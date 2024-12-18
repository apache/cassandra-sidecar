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

import java.util.List;
import java.util.Set;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationProvider;

import static org.apache.cassandra.sidecar.utils.AuthUtils.extractCassandraRoles;

/**
 * Provides authorizations based on user's role. Extracts permissions user holds from Cassandra's
 * system_auth.role_permissions table and from Sidecar's sidecar_internal.role_permissions_v1 table and sets
 * them in user.
 */
public class RoleBasedAuthorizationProvider implements AuthorizationProvider
{
    private final RoleAuthorizationsCache roleAuthorizationsCache;

    public RoleBasedAuthorizationProvider(RoleAuthorizationsCache roleAuthorizationsCache)
    {
        this.roleAuthorizationsCache = roleAuthorizationsCache;
    }

    @Override
    public String getId()
    {
        return "RoleBasedAccessControl";
    }

    @Override
    public void getAuthorizations(User user, Handler<AsyncResult<Void>> handler)
    {
        getAuthorizations(user).onComplete(handler);
    }

    @Override
    public Future<Void> getAuthorizations(User user)
    {
        List<String> roles = extractCassandraRoles(user);

        if (roles.isEmpty())
        {
            return Future.failedFuture("No cassandra roles found associated with the user");
        }

        for (String role : roles)
        {
            if (role == null)
            {
                continue;
            }

            String authorizationId = getId();
            Set<Authorization> authorizations = roleAuthorizationsCache.getAuthorizations(role);
            // when entries in cache are not found, null is returned. We can not add null in user.authorizations()
            if (authorizations != null)
            {
                user.authorizations().add(authorizationId, authorizations);
            }
        }
        return Future.succeededFuture();
    }
}
