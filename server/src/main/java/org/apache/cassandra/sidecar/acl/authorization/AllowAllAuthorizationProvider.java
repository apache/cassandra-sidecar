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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.auth.authorization.AuthorizationProvider;

/**
 * {@link AuthorizationProvider} implementation to allow all requests regardless of authorizations user holds.
 */
public class AllowAllAuthorizationProvider implements AuthorizationProvider
{
    final Authorization authorization;

    public AllowAllAuthorizationProvider()
    {
        // Authorization that always allows
        authorization = new Authorization()
        {
            @Override
            public boolean match(AuthorizationContext context)
            {
                return true;
            }

            @Override
            public boolean verify(Authorization authorization)
            {
                return true;
            }
        };
    }

    /**
     * @return unique id representing {@code AllowAllAuthorizationProvider}
     */
    @Override
    public String getId()
    {
        return "AllowAll";
    }

    @Override
    public void getAuthorizations(User user, Handler<AsyncResult<Void>> handler)
    {
        getAuthorizations(user).onComplete(handler);
    }

    @Override
    public Future<Void> getAuthorizations(User user)
    {
        if (user == null)
        {
            return Future.failedFuture("User cannot be null");
        }

        user.authorizations().add(getId(), authorization);
        return Future.succeededFuture();
    }
}
