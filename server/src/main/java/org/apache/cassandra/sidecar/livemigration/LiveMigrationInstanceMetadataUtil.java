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

package org.apache.cassandra.sidecar.livemigration;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_COMMITLOG_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_HINTS_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_SAVED_CACHES_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.CDC_RAW_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.COMMITLOG_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.HINTS_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.SAVED_CACHES_DIR_PLACEHOLDER;

/**
 * Utility class for having all {@link InstanceMetadata} related helper functions related to
 * Live Migration in one place.
 */
@SuppressWarnings("ConstantValue")
public class LiveMigrationInstanceMetadataUtil
{

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationInstanceMetadataUtil.class);

    /**
     * List of directories Live Migration considers to transfer between source and destination.
     */
    public static List<String> dirsToCopy(InstanceMetadata instanceMetadata)
    {
        List<String> dirsToCopy = new ArrayList<>();
        dirsToCopy.add(instanceMetadata.hintsDir());
        dirsToCopy.add(instanceMetadata.commitlogDir());
        if (instanceMetadata.savedCachesDir() != null)
        {
            dirsToCopy.add(instanceMetadata.savedCachesDir());
        }
        if (instanceMetadata.cdcDir() != null)
        {
            dirsToCopy.add(instanceMetadata.cdcDir());
        }
        if (instanceMetadata.localSystemDataFileDir() != null)
        {
            dirsToCopy.add(instanceMetadata.localSystemDataFileDir());
        }
        dirsToCopy.addAll(instanceMetadata.dataDirs());
        return Collections.unmodifiableList(dirsToCopy);
    }

    /**
     * Returns map of directory that can be copied and the index to use while constructing
     * url for file transfer.
     */
    public static Map<String, String> dirPathPrefixMap(InstanceMetadata instanceMetadata)
    {
        Map<String, String> dirIndexMap = new HashMap<>();
        dirIndexMap.put(instanceMetadata.hintsDir(), LIVE_MIGRATION_HINTS_DIR_PATH + "/0");
        dirIndexMap.put(instanceMetadata.commitlogDir(), LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0");
        if (instanceMetadata.savedCachesDir() != null)
        {
            dirIndexMap.put(instanceMetadata.savedCachesDir(), LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0");
        }
        if (instanceMetadata.cdcDir() != null)
        {
            dirIndexMap.put(instanceMetadata.cdcDir(), LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0");
        }
        if (instanceMetadata.localSystemDataFileDir() != null)
        {
            dirIndexMap.put(instanceMetadata.localSystemDataFileDir(), LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/0");
        }
        for (int i = 0; i < instanceMetadata.dataDirs().size(); i++)
        {
            dirIndexMap.put(instanceMetadata.dataDirs().get(i), LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/" + i);
        }
        return Collections.unmodifiableMap(dirIndexMap);
    }

    /**
     * Returns map of directory and set of placeholders that represent the directory.
     */
    public static Map<String, Set<String>> dirPlaceHoldersMap(InstanceMetadata instanceMetadata)
    {
        Map<String, Set<String>> placeholderMap = new HashMap<>();

        placeholderMap.put(instanceMetadata.hintsDir(), Collections.singleton(HINTS_DIR_PLACEHOLDER));
        placeholderMap.put(instanceMetadata.commitlogDir(), Collections.singleton(COMMITLOG_DIR_PLACEHOLDER));
        if (instanceMetadata.savedCachesDir() != null)
        {
            placeholderMap.put(instanceMetadata.savedCachesDir(), Collections.singleton(SAVED_CACHES_DIR_PLACEHOLDER));
        }

        if (instanceMetadata.cdcDir() != null)
        {
            placeholderMap.put(instanceMetadata.cdcDir(), Collections.singleton(CDC_RAW_DIR_PLACEHOLDER));
        }

        if (instanceMetadata.localSystemDataFileDir() != null)
        {
            placeholderMap.put(instanceMetadata.localSystemDataFileDir(),
                               Collections.singleton(LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER));
        }

        List<String> dataDirs = instanceMetadata.dataDirs();
        for (int i = 0; i < dataDirs.size(); i++)
        {
            String dir = dataDirs.get(i);
            Set<String> placeholders = Set.of(DATA_FILE_DIR_PLACEHOLDER, DATA_FILE_DIR_PLACEHOLDER + "_" + i);
            placeholderMap.put(dir, placeholders);
        }

        return Collections.unmodifiableMap(placeholderMap);
    }

    /**
     * Converts given live migration file download URL to local path.
     *
     * @param fileUrl  Live migration file download URL
     * @param metadata Cassandra instance metadata
     * @return local path for given live migration file download URL
     */
    public static String localPath(@NotNull String fileUrl,
                                   @NotNull InstanceMetadata metadata)
    {

        if (fileUrl.contains("/../") || fileUrl.endsWith("/.."))
        {
            String errorMessage = "Tried to access file using relative path " + fileUrl + ".";
            LOGGER.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        Map<String, String> urlToLocalDirMap = migrationUrlLocalDirMap(metadata);
        for (Map.Entry<String, String> entry : urlToLocalDirMap.entrySet())
        {
            if (fileUrl.startsWith(entry.getKey()))
            {
                Objects.requireNonNull(entry.getValue(), () -> "No local path found for url " + fileUrl);
                String relativePath = fileUrl.substring(entry.getKey().length());
                return Paths.get(entry.getValue(), relativePath).toAbsolutePath().toString();
            }
        }

        throw new IllegalArgumentException("File url " + fileUrl + " is unknown.");
    }

    private static Map<String, String> migrationUrlLocalDirMap(InstanceMetadata instanceMetadata)
    {
        Map<String, String> urlToLocalDirMap = new HashMap<>();
        urlToLocalDirMap.put(LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0/", instanceMetadata.commitlogDir());
        urlToLocalDirMap.put(LIVE_MIGRATION_HINTS_DIR_PATH + "/0/", instanceMetadata.hintsDir());
        urlToLocalDirMap.put(LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0/", instanceMetadata.savedCachesDir());

        if (instanceMetadata.cdcDir() != null)
        {
            urlToLocalDirMap.put(LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/", instanceMetadata.cdcDir());
        }
        if (instanceMetadata.localSystemDataFileDir() != null)
        {
            urlToLocalDirMap.put(LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/0/", instanceMetadata.localSystemDataFileDir());
        }

        for (int i = 0; i < instanceMetadata.dataDirs().size(); i++)
        {
            urlToLocalDirMap.put(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/" + i + "/", instanceMetadata.dataDirs().get(0));
        }

        return urlToLocalDirMap;
    }
}
