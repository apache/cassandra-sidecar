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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.util.Map;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;

/**
 * Map of source to destination C* instances to do the Live Migration.
 */
public interface LiveMigrationMap
{
    /**
     * Tells whether given instance is configured as source or not.
     *
     * @param instanceMeta Cassandra instance metadata
     * @return true if given instance is configured as source for live migration.
     */
    default boolean isSource(@NotNull InstanceMetadata instanceMeta)
    {
        Map<String, String> map = getMigrationMap();
        return map != null && map.containsKey(String.valueOf(instanceMeta.host()));
    }

    /**
     * Tells whether given instance is configured as destination or not.
     *
     * @param instanceMeta Cassandra instance metadata
     * @return true if given instance is configured as destination for live migration.
     */
    default boolean isDestination(@NotNull InstanceMetadata instanceMeta)
    {
        Map<String, String> map = getMigrationMap();
        return map != null && map.containsValue(String.valueOf(instanceMeta.host()));
    }

    /**
     * Tells whether given instance is configured as either source or destination.
     *
     * @param instanceMeta Cassandra instance metadata
     * @return true if given instance is either source or destination
     */
    default boolean isAny(@NotNull InstanceMetadata instanceMeta)
    {
        return isSource(instanceMeta) || isDestination(instanceMeta);
    }

    /**
     * Returns map of source and destination
     *
     * @return map of source and destination
     */
    Map<String, String> getMigrationMap();
}
