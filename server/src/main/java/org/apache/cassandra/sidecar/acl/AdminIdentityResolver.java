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

package org.apache.cassandra.sidecar.acl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.acl.authorization.SuperUserCache;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Evaluates if provided identity is an admin identity.
 */
@Singleton
public class AdminIdentityResolver
{
    private final IdentityToRoleCache identityToRoleCache;
    private final SuperUserCache superUserCache;
    private final AccessControlConfiguration config;

    @Inject
    public AdminIdentityResolver(IdentityToRoleCache identityToRoleCache,
                                 SuperUserCache superUserCache,
                                 SidecarConfiguration sidecarConfiguration)
    {
        this.identityToRoleCache = identityToRoleCache;
        this.superUserCache = superUserCache;
        this.config = sidecarConfiguration.accessControlConfiguration();
    }

    public Future<Boolean> isAdmin(String identity)
    {
        if (config.adminIdentities().contains(identity))
        {
            return Future.succeededFuture(true);
        }
        return identityToRoleCache
               .get(identity)
               .compose(role -> {
                   if (role == null)
                   {
                       return Future.succeededFuture(false);
                   }
                   // Cassandra superusers have admin privileges
                   return superUserCache.isSuperUser(role);
               });
    }
}
