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

package org.apache.cassandra.sidecar.config;

import java.util.Map;
import java.util.Set;

/**
 * Configuration for Live Migration feature.
 */
public interface LiveMigrationConfiguration
{

    /**
     * Files to be excluded from Live Migration.
     * @return set of file exclusion patterns.
     */
    Set<String> filesToExclude();

    /**
     * Directories to be excluded from Live Migration.
     * @return set of directory exclusion patterns.
     */
    Set<String> directoriesToExclude();

    /**
     * Map of source and destination Cassandra instances to migrate.
     *
     * @return Map of strings where key is the source instance hostname and value is the destination instance hostname.
     */
    Map<String, String> migrationMap();
}
