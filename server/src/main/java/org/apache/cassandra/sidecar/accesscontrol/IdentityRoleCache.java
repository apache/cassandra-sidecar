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

package org.apache.cassandra.sidecar.accesscontrol;

import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;

/**
 * Caches entries from identity_to_role table. identity_to_role table maps valid certificate identity to Cassandra
 * role the identity holds. identity_to_role table is created in Cassandra versions 5.x and above
 */
public class IdentityRoleCache extends AuthCache<String, String>
{
    protected static final String NAME = "identity_to_role_cache";
    protected final SystemAuthDatabaseAccessor systemAuthDatabaseAccessor;

    public IdentityRoleCache(CacheConfiguration config,
                             SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
    {
        super(NAME,
              config.enabled(),
              systemAuthDatabaseAccessor::findRoleFromIdentity,
              systemAuthDatabaseAccessor::findAllIdentityRoles,
              config.warmingRetries(),
              2000,
              config.expireAfterAccessMillis(),
              config.maximumSize());
        this.systemAuthDatabaseAccessor = systemAuthDatabaseAccessor;
    }

    public boolean contains(String identity)
    {

        if (cache == null)
        {
            return false;
        }
        String role = get(identity);
        return  role != null && !role.isEmpty();
    }
}
