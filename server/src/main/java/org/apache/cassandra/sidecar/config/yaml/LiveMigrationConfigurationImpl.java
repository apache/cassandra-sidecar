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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;

/**
 * Configuration required for live migrating the Cassandra instances.
 */
public class LiveMigrationConfigurationImpl implements LiveMigrationConfiguration
{

    public static final int DEFAULT_MAX_CONCURRENT_FILE_REQUESTS = 20;

    private final Set<String> filesToExclude;
    private final Set<String> directoriesToExclude;
    private final Map<String, String> migrationMap;
    private final int maxConcurrentFileRequests;

    public LiveMigrationConfigurationImpl()
    {
        this(Collections.emptySet(), Collections.emptySet(), Collections.emptyMap(), DEFAULT_MAX_CONCURRENT_FILE_REQUESTS);
    }

    @JsonCreator
    public LiveMigrationConfigurationImpl(@JsonProperty("files_to_exclude") Set<String> filesToExclude,
                                          @JsonProperty("dirs_to_exclude") Set<String> directoriesToExclude,
                                          @JsonProperty("migration_map") Map<String, String> migrationMap,
                                          @JsonProperty("max_concurrent_file_requests") int maxConcurrentFileRequests)
    {
        Map<String, String> migrationMapOrEmpty = migrationMap == null
                                                  ? Collections.emptyMap()
                                                  : Collections.unmodifiableMap(migrationMap);
        validateMigrationMap(migrationMapOrEmpty);
        if (maxConcurrentFileRequests < 1)
        {
            throw new IllegalArgumentException("Invalid max_concurrent_file_requests " + maxConcurrentFileRequests +
                                               ". It must be >= 1");
        }

        this.filesToExclude = filesToExclude;
        this.directoriesToExclude = directoriesToExclude;
        this.migrationMap = migrationMapOrEmpty;
        this.maxConcurrentFileRequests = maxConcurrentFileRequests;
    }

    private static void validateMigrationMap(Map<String, String> migrationMap)
    {
        Set<String> sources = migrationMap.keySet();
        Set<String> destinations = new HashSet<>();
        Set<String> duplicateDestinations = new HashSet<>();
        for (String destination : migrationMap.values())
        {
            if (!destinations.add(destination))
            {
                duplicateDestinations.add(destination);
            }
        }

        if (!duplicateDestinations.isEmpty())
        {
            throw new IllegalArgumentException("Invalid migration_map: a node cannot be the destination of multiple " +
                                               "sources. Duplicate destinations: " + duplicateDestinations);
        }

        Set<String> sourceAndDestination = new HashSet<>();
        for (String source : sources)
        {
            if (destinations.contains(source))
            {
                sourceAndDestination.add(source);
            }
        }
        if (!sourceAndDestination.isEmpty())
        {
            throw new IllegalArgumentException("Invalid migration_map: a node cannot be both source and destination. " +
                                               "Conflicting nodes: " + sourceAndDestination);
        }
    }

    @Override
    @JsonProperty("files_to_exclude")
    public Set<String> filesToExclude()
    {
        return filesToExclude;
    }

    @Override
    @JsonProperty("dirs_to_exclude")
    public Set<String> directoriesToExclude()
    {
        return directoriesToExclude;
    }

    @Override
    @JsonProperty("migration_map")
    public Map<String, String> migrationMap()
    {
        return migrationMap;
    }

    @Override
    @JsonProperty("max_concurrent_file_requests")
    public int maxConcurrentFileRequests()
    {
        return maxConcurrentFileRequests;
    }
}
