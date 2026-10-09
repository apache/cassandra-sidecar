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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.UnknownMigrationPrefixException;
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
     * Resolves a live migration file download URL to a local path. Performs only a lexical
     * containment check; symlinks are not resolved and the file is not required to exist.
     * Suitable for destination-side callers that place files into operator-controlled directories.
     * Source-side callers that serve existing files in response to remote requests must additionally
     * call {@link ResolvedPath#verifyContainment()} on the returned object before using the path.
     *
     * @param fileUrl  Live migration file download URL
     * @param metadata Cassandra instance metadata
     * @return lexically-resolved local path for given live migration file download URL
     * @throws IllegalArgumentException if the URL is malformed or the lexical path escapes
     *                                  the base directory
     */
    public static Path localPath(@NotNull String fileUrl,
                                 @NotNull InstanceMetadata metadata)
    {
        return resolveLexically(fileUrl, metadata).resolvedPath();
    }

    /**
     * Lexically resolves a live migration file download URL to a {@link ResolvedPath}. Performs only
     * string-level validation; the filesystem is not touched and the file is not required to exist.
     *
     * @param fileUrl  Live migration file download URL
     * @param metadata Cassandra instance metadata
     * @return the lexically-resolved {@link ResolvedPath}
     * @throws IllegalArgumentException        if the URL is malformed - it contains a relative traversal
     *                                         segment ({@code /../}) or lexically escapes the configured base directory
     * @throws UnknownMigrationPrefixException if the URL does not match any configured live-migration directory
     *                                         prefix; the URL is well-formed but addresses no resource on this
     *                                         instance. This is a subtype of {@link IllegalArgumentException}, so
     *                                         callers that only care about "bad URL" need not distinguish it
     */
    public static ResolvedPath resolveLexically(@NotNull String fileUrl,
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

                return new ResolvedPath(baseDir, resolvedPath);
            }
        }

        LOGGER.warn("File url {} does not match any configured live-migration directory prefix.", fileUrl);
        throw new UnknownMigrationPrefixException("File url " + fileUrl + " is unknown.");
    }

    /**
     * Lexical resolution of a live-migration URL: the configured base directory paired with the
     * local path it maps to.
     */
    public static final class ResolvedPath
    {
        private final Path baseDir;
        private final Path resolvedPath;

        ResolvedPath(Path baseDir, Path resolvedPath)
        {
            this.baseDir = baseDir;
            this.resolvedPath = resolvedPath;
        }

        public Path resolvedPath()
        {
            return resolvedPath;
        }

        /**
         * Verifies that the source-side file represented by this {@code ResolvedPath} exists and
         * that its real (symlinks resolved) path stays inside the real base directory path.
         * Use this on the source side after
         * {@link LiveMigrationInstanceMetadataUtil#resolveLexically(String, InstanceMetadata)} to
         * enforce that the file the operator is about to serve cannot escape the configured
         * directory through a symlink. Callers continue to use {@link #resolvedPath()} (the
         * lexical form) for exclusion matching, logging, and serving, since exclusion patterns
         * and operator-facing logs are configured against the lexical form.
         *
         * <p>Performs blocking filesystem I/O - must be called from a worker thread, not the event
         * loop. Throws {@link NoSuchFileException} before any real-path resolution runs, so
         * callers can distinguish "file missing" from "path escapes via symlink". The base directory
         * is only resolved when the file's real path does not already start with it, so a base
         * directory that is not itself a symlink costs no extra filesystem calls.
         *
         * @throws NoSuchFileException      if the resolved file does not exist
         * @throws IOException              if {@link Path#toRealPath} fails for an I/O reason
         *                                  other than missing file
         * @throws IllegalArgumentException if the real path escapes the base directory
         */
        public void verifyContainment() throws IOException
        {
            if (!Files.exists(resolvedPath))
            {
                throw new NoSuchFileException(resolvedPath.toString());
            }
            Path realPath = resolvedPath.toRealPath();
            // When the base directory is not itself behind a symlink, the file's real path still
            // starts with the lexical base directory, which already proves containment.
            if (realPath.startsWith(baseDir))
            {
                return;
            }
            // The base directory itself may be a symlink (for example a data dir pointing at a
            // mounted volume), so compare against its real path before rejecting.
            if (!realPath.startsWith(baseDir.toRealPath()))
            {
                LOGGER.error("Resolved path escapes base directory for {}", resolvedPath);
                throw new IllegalArgumentException("Resolved path escapes base directory");
            }
        }
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
