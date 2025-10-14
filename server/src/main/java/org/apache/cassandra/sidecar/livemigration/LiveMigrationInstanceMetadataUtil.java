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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.CDC_RAW_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.COMMIT_LOG_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.DATA_FILE_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.HINTS_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.LOCAL_SYSTEM_DATA_FILE_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.SAVED_CACHES_DIR;
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
public class LiveMigrationInstanceMetadataUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationInstanceMetadataUtil.class);

    /**
     * Encapsulates all metadata for a specific directory instance in live migration.
     * Each descriptor represents one physical directory with its associated index, URL route, and placeholder.
     *
     * <p>Example mapping for a data directory at index 0:
     * <ul>
     *   <li>dirType: DATA_FILE_DIR</li>
     *   <li>placeholder: "DATA_FILE_DIR"</li>
     *   <li>localDir: "/var/lib/cassandra/data"</li>
     *   <li>index: 0</li>
     *   <li>fileTransferUrl: "/api/v1/live-migration/data/0"</li>
     * </ul>
     *
     * <p>The descriptor is used to map between:
     * <ul>
     *   <li>URL paths (e.g., /api/v1/live-migration/data/0/keyspace/table/file.db)</li>
     *   <li>Local file paths (e.g., /var/lib/cassandra/data/keyspace/table/file.db)</li>
     *   <li>Configuration placeholders (e.g., ${DATA_FILE_DIR}/keyspace/table/file.db)</li>
     * </ul>
     */
    private static class DirectoryDescriptor
    {
        /** The type of directory (e.g., DATA_FILE_DIR, HINTS_DIR, COMMIT_LOG_DIR) */
        final LiveMigrationDirType dirType;

        /** The placeholder name used in configuration patterns (e.g., "DATA_FILE_DIR") */
        final String placeholder;

        /** The absolute path to the local directory on the file system */
        final String localDir;

        /** The index of this directory within directories of the same type (0-based) */
        final int index;

        /** The URL prefix for file transfer operations (e.g., "/api/v1/live-migration/data/0") */
        final String fileTransferUrl;

        /**
         * Creates a directory descriptor with the specified metadata.
         *
         * @param dirType the type of directory
         * @param placeholder the placeholder name for configuration patterns
         * @param localDir the absolute path to the local directory
         * @param index the index of this directory (0 for single directories, 0-N for data directories)
         */
        DirectoryDescriptor(LiveMigrationDirType dirType,
                            String placeholder,
                            String localDir,
                            int index)
        {
            this.dirType = dirType;
            this.placeholder = placeholder;
            this.localDir = localDir;
            this.index = index;

            String urlBase = ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE + "/" + dirType.dirType;
            this.fileTransferUrl = urlBase + "/" + index;
        }

        /**
         * Returns the set of placeholders that can be used to reference this directory.
         *
         * <p>Most directory types return a single placeholder (e.g., "HINTS_DIR").
         * DATA_FILE_DIR returns both a generic placeholder and an indexed one to support
         * multiple data directories:
         * <ul>
         *   <li>Generic: "DATA_FILE_DIR" - resolves to all data directories</li>
         *   <li>Indexed: "DATA_FILE_DIR_0", "DATA_FILE_DIR_1", etc. - resolves to specific data directory</li>
         * </ul>
         *
         * @return immutable set of placeholder strings for this directory
         */
        Set<String> getPlaceholders()
        {
            if (dirType == DATA_FILE_DIR)
            {
                return Set.of(placeholder, placeholder + "_" + index);
            }
            return Set.of(placeholder);
        }
    }

    /**
     * Builds all directory descriptors for the given instance metadata.
     * Each descriptor represents one specific directory with its index.
     * This is the single source of truth for all directory-to-URL mappings.
     *
     * @param instanceMetadata the Cassandra instance metadata
     * @return list of directory descriptors for all directories in the instance
     */
    @SuppressWarnings("ConstantValue")
    private static List<DirectoryDescriptor> buildDescriptors(InstanceMetadata instanceMetadata)
    {
        List<DirectoryDescriptor> descriptors = new ArrayList<>();

        // Hints directory - always has index 0
        descriptors.add(new DirectoryDescriptor(
            HINTS_DIR, HINTS_DIR_PLACEHOLDER, instanceMetadata.hintsDir(), 0));

        // Commit log directory - always has index 0
        descriptors.add(new DirectoryDescriptor(
            COMMIT_LOG_DIR, COMMITLOG_DIR_PLACEHOLDER, instanceMetadata.commitlogDir(), 0));

        // Saved caches directory - always has index 0
        if (instanceMetadata.savedCachesDir() != null)
        {
            descriptors.add(new DirectoryDescriptor(
                SAVED_CACHES_DIR, SAVED_CACHES_DIR_PLACEHOLDER, instanceMetadata.savedCachesDir(), 0));
        }

        // CDC directory - always has index 0
        if (instanceMetadata.cdcDir() != null)
        {
            descriptors.add(new DirectoryDescriptor(
                CDC_RAW_DIR, CDC_RAW_DIR_PLACEHOLDER, instanceMetadata.cdcDir(), 0));
        }

        // Local system data directory - always has index 0
        if (instanceMetadata.localSystemDataFileDir() != null)
        {
            descriptors.add(new DirectoryDescriptor(
                LOCAL_SYSTEM_DATA_FILE_DIR, LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER,
                instanceMetadata.localSystemDataFileDir(), 0));
        }

        // Data directories - each gets its own index
        List<String> dataDirs = instanceMetadata.dataDirs();
        for (int i = 0; i < dataDirs.size(); i++)
        {
            descriptors.add(new DirectoryDescriptor(
                DATA_FILE_DIR, DATA_FILE_DIR_PLACEHOLDER, dataDirs.get(i), i));
        }

        return descriptors;
    }

    /**
     * Returns all directories that need to be copied during live migration.
     * Includes hints, commit log, saved caches, CDC, local system data, and data directories.
     *
     * @param instanceMetadata the Cassandra instance metadata containing directory paths
     * @return an unmodifiable list of directory paths to be copied during live migration
     */
    public static List<String> dirsToCopy(InstanceMetadata instanceMetadata)
    {
        List<String> dirsToCopy = new ArrayList<>();
        for (DirectoryDescriptor desc : buildDescriptors(instanceMetadata))
        {
            dirsToCopy.add(desc.localDir);
        }
        return Collections.unmodifiableList(dirsToCopy);
    }

    /**
     * Returns a map of local directory paths to their corresponding file transfer URL prefixes.
     * <p>
     * Example: For a data directory "/var/lib/cassandra/data", the map contains:
     * "/var/lib/cassandra/data" -&gt; "/api/v1/live-migration/data/0"
     *
     * @param instanceMetadata the Cassandra instance metadata containing directory paths
     * @return unmodifiable map from local directory path to URL prefix
     */
    public static Map<String, String> dirPathPrefixMap(InstanceMetadata instanceMetadata)
    {
        Map<String, String> dirIndexMap = new HashMap<>();
        for (DirectoryDescriptor desc : buildDescriptors(instanceMetadata))
        {
            dirIndexMap.put(desc.localDir, desc.fileTransferUrl);
        }
        return Collections.unmodifiableMap(dirIndexMap);
    }

    /**
     * Returns map of directory and set of placeholders that represent the directory.
     */
    public static Map<String, Set<String>> dirPlaceHoldersMap(InstanceMetadata instanceMetadata)
    {
        Map<String, Set<String>> placeholderMap = new HashMap<>();
        for (DirectoryDescriptor desc : buildDescriptors(instanceMetadata))
        {
            placeholderMap.put(desc.localDir, desc.getPlaceholders());
        }
        return Collections.unmodifiableMap(placeholderMap);
    }

    /**
     * Returns a map of placeholder and its directories based on given {@link InstanceMetadata}.
     */
    public static Map<String, Set<String>> placeholderDirsMap(InstanceMetadata instanceMetadata)
    {
        Map<String, Set<String>> placeholderDirsMap = new HashMap<>();

        for (DirectoryDescriptor desc : buildDescriptors(instanceMetadata))
        {
            for (String placeholder : desc.getPlaceholders())
            {
                placeholderDirsMap.computeIfAbsent(placeholder, k -> new HashSet<>())
                                  .add(desc.localDir);
            }
        }

        return Collections.unmodifiableMap(placeholderDirsMap);
    }

    /**
     * Converts given live migration file download URL to local path.
     *
     * @param fileUrl  Live migration file download URL
     * @param metadata Cassandra instance metadata
     * @return local path for given live migration file download URL
     */
    public static Path localPath(@NotNull String fileUrl,
                                 @NotNull InstanceMetadata metadata)
    {
        Objects.requireNonNull(fileUrl, "fileUrl cannot be null");
        Objects.requireNonNull(metadata, "metadata cannot be null");

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
                Path baseDir = Paths.get(entry.getValue()).toAbsolutePath().normalize();
                Path resolvedPath = Paths.get(entry.getValue(), relativePath).toAbsolutePath().normalize();

                if (!resolvedPath.startsWith(baseDir))
                {
                    String errorMessage = "Resolved path escapes base directory for url " + fileUrl;
                    LOGGER.error(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }

                return resolvedPath;
            }
        }

        throw new IllegalArgumentException("File url " + fileUrl + " is unknown.");
    }

    private static Map<String, String> migrationUrlLocalDirMap(InstanceMetadata instanceMetadata)
    {
        Map<String, String> urlToLocalDirMap = new HashMap<>();

        for (DirectoryDescriptor desc : buildDescriptors(instanceMetadata))
        {
            // Add file transfer URL mapping
            urlToLocalDirMap.put(desc.fileTransferUrl + "/", desc.localDir);
        }

        return Collections.unmodifiableMap(urlToLocalDirMap);
    }
}
