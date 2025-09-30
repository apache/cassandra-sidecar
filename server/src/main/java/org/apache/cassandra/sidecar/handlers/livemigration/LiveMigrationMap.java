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

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
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
     * @return Future that completes with true if given instance is configured as source for live migration.
     */
    default Future<Boolean> isSource(@NotNull InstanceMetadata instanceMeta)
    {
        return getMigrationMap()
               .compose(migrationMap -> Future.succeededFuture(migrationMap.containsKey(instanceMeta.host())));
    }

    /**
     * Tells whether given instance is configured as destination or not.
     *
     * @param instanceMeta Cassandra instance metadata
     * @return Future that completes with true if given instance is configured as destination for live migration.
     */
    default Future<Boolean> isDestination(@NotNull InstanceMetadata instanceMeta)
    {
        return getMigrationMap()
               .compose(migrationMap -> Future.succeededFuture(migrationMap.containsValue(instanceMeta.host())));
    }

    /**
     * Tells whether given instance is configured as either source or destination.
     *
     * @param instanceMeta Cassandra instance metadata
     * @return Future that completes with true if given instance is either source or destination
     */
    default Future<Boolean> isAny(@NotNull InstanceMetadata instanceMeta)
    {
        return Future.join(isSource(instanceMeta), isDestination(instanceMeta))
                     .compose(results -> {
                         boolean any = Boolean.TRUE.equals(results.resultAt(0))
                                       || Boolean.TRUE.equals(results.resultAt(1));
                         return Future.succeededFuture(any);
                     });
    }

    /**
     * Returns map of source and destination. Returns empty map if live migration is not configured.
     *
     * @return Future that completes with map of source and destination
     */
    @NotNull
    Future<Map<String, String>> getMigrationMap();


    /**
     * Retrieves the source instance host for a given destination host in the live migration mapping.
     * This method performs a reverse lookup in the migration map to find which source host
     * corresponds to the specified destination host.
     *
     * @param destination the destination host address for which to find the corresponding source host.
     *                    Must not be null.
     * @return Future that completes with the source host address mapped to the given destination host,
     *         or fails with LiveMigrationInvalidRequestException if live migration is not configured
     *         or if no source host is found for the given destination host
     */
    default Future<String> getSource(String destination)
    {

        if (null == destination)
        {
            return Future.failedFuture(new IllegalArgumentException("Destination must not be null"));
        }

        return getMigrationMap()
               .compose(migrationMap -> {
                   if (null == migrationMap || migrationMap.isEmpty())
                   {
                       return Future.failedFuture(new LiveMigrationInvalidRequestException("Live migration is not configured"));
                   }

                   for (Map.Entry<String, String> entry : migrationMap.entrySet())
                   {
                       if (entry.getValue().equals(destination))
                       {
                           return Future.succeededFuture(entry.getKey());
                       }
                   }

                   // Reached here means migration not configured for given destination
                   return Future.failedFuture(new LiveMigrationInvalidRequestException("No source for destination " + destination + " is present " +
                                                                                       "in the migration map"));
               });
    }
}
