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

package org.apache.cassandra.sidecar.metrics.server;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.metrics.CacheStatsCounter;

import static org.apache.cassandra.sidecar.handlers.snapshots.ListSnapshotHandler.SNAPSHOT_CACHE_NAME;

/**
 * Tracks metrics related to internal caches.
 */
public class CacheMetrics
{
    public final CacheStatsCounter snapshotCacheMetrics;
    public final CacheStatsCounter identityToRoleCacheMetrics;
    public final CacheStatsCounter superUserCacheMetrics;
    public final CacheStatsCounter rolePermissionsCacheMetrics;
    public final CacheStatsCounter authorizationCacheMetrics;

    public CacheMetrics(MetricRegistry globalMetricRegistry)
    {
        snapshotCacheMetrics = new CacheStatsCounter(globalMetricRegistry, SNAPSHOT_CACHE_NAME);
        identityToRoleCacheMetrics = new CacheStatsCounter(globalMetricRegistry, "identity_to_role_cache");
        superUserCacheMetrics = new CacheStatsCounter(globalMetricRegistry, "super_user_cache");
        rolePermissionsCacheMetrics = new CacheStatsCounter(globalMetricRegistry, "role_permissions_cache");
        authorizationCacheMetrics = new CacheStatsCounter(globalMetricRegistry, "authorization_cache");
    }
}
