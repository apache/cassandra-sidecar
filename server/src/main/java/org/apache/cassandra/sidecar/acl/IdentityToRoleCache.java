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
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;

/**
 * Caches entries from system_auth.identity_to_role table. The table maps valid certificate identities to Cassandra
 * roles. identity_to_role table is available since Cassandra versions 5.0
 */
@Singleton
public class IdentityToRoleCache extends AuthCache<String, String>
{
    private static final String NAME = "identity_to_role_cache";

    @Inject
    public IdentityToRoleCache(Vertx vertx,
                               ExecutorPools executorPools,
                               SidecarConfiguration sidecarConfiguration,
                               SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
    {
        super(NAME,
              vertx,
              executorPools,
              systemAuthDatabaseAccessor::findRoleFromIdentity,
              systemAuthDatabaseAccessor::findAllIdentityToRoles,
              sidecarConfiguration.accessControlConfiguration().permissionCacheConfiguration());
    }

    public boolean containsKey(String identity)
    {
        try
        {
            return cache() != null && get(identity) != null;
        }
        catch (SchemaUnavailableException e)
        {
            // Returns false, since SystemAuthSchema is not prepared
            return false;
        }
    }
}
