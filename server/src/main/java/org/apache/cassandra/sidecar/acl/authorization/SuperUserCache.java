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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.acl.AuthCache;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;

/**
 * Caches superuser status of cassandra roles. Returns true if the supplied role or any other role granted to it
 * (directly or indirectly) has superuser status.
 * Note: {@link SuperUserCache} maintains only the superuser status. It can not guarantee whether a role exists
 */
@Singleton
public class SuperUserCache extends AuthCache<String, Boolean>
{
    public static final String NAME = "super_user_cache";

    @Inject
    public SuperUserCache(Vertx vertx,
                          ExecutorPools executorPools,
                          SidecarConfiguration sidecarConfiguration,
                          SystemAuthDatabaseAccessor systemAuthDatabaseAccessor,
                          SidecarMetrics sidecarMetrics)
    {
        super(NAME,
              vertx,
              executorPools,
              systemAuthDatabaseAccessor::isSuperUser,
              systemAuthDatabaseAccessor::findAllRolesToSuperuserStatus,
              sidecarConfiguration.accessControlConfiguration().permissionCacheConfiguration(),
              sidecarMetrics.server().cache().superUserCacheMetrics);
    }

    public Future<Boolean> isSuperUser(String role)
    {
        return get(role).map(status -> status != null && status);
    }
}
