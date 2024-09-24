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

package org.apache.cassandra.sidecar.metrics;

import org.apache.cassandra.sidecar.db.schema.SidecarSchemaInitializer;

import static org.apache.cassandra.sidecar.metrics.SidecarMetrics.APP_PREFIX;

/**
 * {@link ServerMetrics} tracks metrics related to Sidecar server.
 */
public interface ServerMetrics
{
    String SERVER_PREFIX = APP_PREFIX + ".Server";

    /**
     * @return health metrics tracked for Sidecar server.
     */
    HealthMetrics health();

    /**
     * @return metrics tracked for resources maintained by Sidecar server.
     */
    ResourceMetrics resource();

    /**
     * @return metrics tracked by server for restore functionality.
     */
    RestoreMetrics restore();

    /**
     * @return metrics related to {@link SidecarSchemaInitializer} that are tracked.
     */
    SchemaMetrics schema();

    /**
     * @return metrics related to internal caches that are tracked.
     */
    CacheMetrics cacheMetrics();
}
