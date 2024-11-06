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
 * Caches entries from system_auth.identity_to_role table. The table maps valid certificate identities to Cassandra
 * roles. identity_to_role table is available since Cassandra versions 5.0
 */
public class IdentityToRoleCache extends AuthCache<String, String>
{
    protected static final String NAME = "identity_to_role_cache";
    protected final SystemAuthDatabaseAccessor systemAuthDatabaseAccessor;

    public IdentityToRoleCache(CacheConfiguration config,
                               SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
    {
        super(NAME,
              systemAuthDatabaseAccessor::findRoleFromIdentity,
              systemAuthDatabaseAccessor::findAllIdentityToRoles,
              config);
        this.systemAuthDatabaseAccessor = systemAuthDatabaseAccessor;
    }

    public boolean containsKey(String identity)
    {
        if (cache == null)
        {
            return false;
        }
        String role = get(identity);
        return  role != null && !role.isEmpty();
    }
}
