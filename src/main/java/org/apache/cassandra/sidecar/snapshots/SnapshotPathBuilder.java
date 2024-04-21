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

package org.apache.cassandra.sidecar.snapshots;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.AbstractMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.utils.BaseFileSystem;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * This class builds the snapshot path on a given host validating that it exists
 */
@Singleton
public class SnapshotPathBuilder extends BaseFileSystem
{
    public static final String SNAPSHOTS_DIR_NAME = "snapshots";

    /**
     * Creates a new SnapshotPathBuilder for snapshots of an instance with the given {@code vertx} instance and
     * {@code instancesConfig Cassandra configuration}.
     *
     * @param vertx           the vertx instance
     * @param instancesConfig the configuration for Cassandra
     * @param validator       a validator instance to validate Cassandra-specific input
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    public SnapshotPathBuilder(Vertx vertx,
                               InstancesConfig instancesConfig,
                               CassandraInputValidator validator,
                               ExecutorPools executorPools)
    {
        super(vertx.fileSystem(), instancesConfig, validator, executorPools);
    }

    /**
     * Returns a Future with a stream of {@link SnapshotFile}s from the list of table data directories for a given
     * {@code snapshotName}. Secondary index files will be included when {@code includeSecondaryIndexFiles} is
     * set to {@code true}.
     *
     * @param tableDataDirectoryList     the list of table data directory files
     * @param snapshotName               the name of the snapshot
     * @param includeSecondaryIndexFiles whether to include secondary index files
     * @return a {@link Future} with a stream of {@link SnapshotFile}s from the list of table data directories for
     * a given {@code snapshotName}
     */
    public Future<Stream<SnapshotFile>> streamSnapshotFiles(List<String> tableDataDirectoryList,
                                                            String snapshotName,
                                                            boolean includeSecondaryIndexFiles)
    {
        return executorPools.internal().executeBlocking(() -> {
            return IntStream.range(0, tableDataDirectoryList.size())
                            // Get the index and resolved snapshot directory
                            .mapToObj(dataDirIndex -> {
                                String dataDir = tableDataDirectoryList.get(dataDirIndex);
                                Path snapshotDir = Paths.get(dataDir)
                                                        .resolve(SNAPSHOTS_DIR_NAME)
                                                        .resolve(snapshotName);
                                return pair(dataDirIndex, snapshotDir);
                            })
                            // The snapshot directory might not exist on every data directory.
                            // For example, if there was only one row inserted in a table,
                            // and we have 4 data directories, only a single data directory
                            // will have SSTables, and only that data directory will create the
                            // snapshot directory.
                            .filter(entry -> Files.exists(entry.getValue()) && Files.isDirectory(entry.getValue()))
                            // List all the files in the directory
                            .flatMap(entry -> listSnapshotDir(entry.getKey(), entry.getValue(), includeSecondaryIndexFiles));
        });
    }

    protected Stream<SnapshotFile> listSnapshotDir(int dataDirectoryIndex, Path snapshotDir,
                                                   boolean includeSecondaryIndexFiles)
    {
        String tableId = tableId(snapshotDir);
        int snapshotDirNameCount = snapshotDir.getNameCount();
        try
        {
            // flatmap will close the stream opened by Files.walk. From javadocs: "Each mapped stream is
            // closed after its contents have been placed into this stream." See:
            // java.util.stream.ReferencePipeline#flatMap
            return Files.walk(snapshotDir, includeSecondaryIndexFiles ? 2 : 1)
                        .map(snapshotFile -> {
                            try
                            {
                                BasicFileAttributes attrs = Files.readAttributes(snapshotFile, BasicFileAttributes.class);
                                if (!attrs.isRegularFile())
                                {
                                    return null;
                                }

                                String snapshotFileName = snapshotFile.subpath(snapshotDirNameCount, snapshotFile.getNameCount()).toString();
                                return new SnapshotFile(snapshotFileName, snapshotFile, attrs.size(), dataDirectoryIndex, tableId);
                            }
                            catch (IOException e)
                            {
                                throw new RuntimeException("Unable to read file attributes for " + snapshotFile, e);
                            }
                        })
                        .filter(Objects::nonNull);
        }
        catch (IOException ioException)
        {
            throw new RuntimeException("Unable to list directory " + snapshotDir, ioException);
        }
    }

    /**
     * Validates that the component name is either {@code *.db} or a {@code *-TOC.txt}
     * which are the only required components to read SSTables.
     *
     * @param request the request to stream the SSTable component
     */
    protected void validate(StreamSSTableComponentRequest request)
    {
        validator.validateKeyspaceName(request.keyspace());
        validator.validateTableName(request.tableName());
        if (request.tableId() != null)
        {
            validator.validateTableId(request.tableId());
        }
        validator.validateSnapshotName(request.snapshotName());
        // Only allow .db and TOC.txt components here
        String secondaryIndexName = request.secondaryIndexName();
        if (secondaryIndexName != null)
        {
            Preconditions.checkArgument(!secondaryIndexName.isEmpty(), "secondaryIndexName cannot be empty");
            Preconditions.checkArgument(secondaryIndexName.charAt(0) == '.', "Invalid secondary index name");
            String indexName = secondaryIndexName.substring(1);
            validator.validatePattern(indexName, indexName, "secondary index", false);
        }
        validator.validateRestrictedComponentName(request.componentName());
    }

    /**
     * @param dataDirectory the path to the data directory where the component lives
     * @param request       the {@link StreamSSTableComponentRequest}
     * @return the path to the component found in the {@code dataDirectory}
     */
    public String resolveComponentPathFromDataDirectory(String dataDirectory,
                                                        StreamSSTableComponentRequest request)
    {
        validate(request);
        StringBuilder sb = new StringBuilder(StringUtils.removeEnd(dataDirectory, File.separator))
                           .append(File.separator).append(request.keyspace())
                           .append(File.separator).append(request.tableName());
        if (request.tableId() != null)
        {
            sb.append("-").append(request.tableId());
        }
        return appendSnapshot(sb, request);
    }

    /**
     * @param tableDirectory the path to the table directory where the component lives
     * @param request        the {@link StreamSSTableComponentRequest}
     * @return the path to the component found in the {@code tableDirectory}
     */
    public String resolveComponentPathFromTableDirectory(String tableDirectory,
                                                         StreamSSTableComponentRequest request)
    {
        validate(request);
        StringBuilder sb = new StringBuilder(StringUtils.removeEnd(tableDirectory, File.separator));
        return appendSnapshot(sb, request);
    }

    /**
     * Removes the table UUID portion from the table name if present.
     *
     * @param tableName the table name with or without the UUID
     * @return the table name without the UUID
     */
    public String maybeRemoveTableId(String tableName)
    {
        int dashIndex = tableName.lastIndexOf("-");
        if (dashIndex > 0)
        {
            return tableName.substring(0, dashIndex);
        }
        return tableName;
    }

    /**
     * @param snapshotDir the snapshot directory
     * @return the unique table identifier
     */
    protected String tableId(@NotNull Path snapshotDir)
    {
        Path fileName = snapshotDir.getName(snapshotDir.getNameCount() - 3).getFileName();
        if (fileName == null)
        {
            return null;
        }

        String tableDirectory = fileName.toString();
        int index = tableDirectory.indexOf("-");
        if (index > 0 && index + 1 < tableDirectory.length())
        {
            return tableDirectory.substring(index + 1);
        }
        return null;
    }

    protected String appendSnapshot(StringBuilder sb, StreamSSTableComponentRequest request)
    {
        sb.append(File.separator).append(SNAPSHOTS_DIR_NAME)
          .append(File.separator).append(request.snapshotName());
        if (request.secondaryIndexName() != null)
        {
            sb.append(File.separator).append(request.secondaryIndexName());
        }
        return sb.append(File.separator).append(request.componentName()).toString();
    }

    /**
     * Class representing a snapshot component file
     */
    public static class SnapshotFile
    {
        public final String name;
        public final Path path;
        public final long size;
        public final int dataDirectoryIndex;
        public final String tableId;

        @VisibleForTesting
        SnapshotFile(Path path, long size, int dataDirectoryIndex, String tableId)
        {
            this(Objects.requireNonNull(path.getFileName(), "path.getFileName() cannot be null").toString(),
                 path, size, dataDirectoryIndex, tableId);
        }

        SnapshotFile(String name, Path path, long size, int dataDirectoryIndex, String tableId)
        {
            this.name = name;
            this.path = path;
            this.size = size;
            this.dataDirectoryIndex = dataDirectoryIndex;
            this.tableId = tableId;
        }

        @Override
        public String toString()
        {
            return "SnapshotFile{" +
                   "name='" + name + '\'' +
                   ", path=" + path +
                   ", size=" + size +
                   ", dataDirectoryIndex=" + dataDirectoryIndex +
                   ", tableId='" + tableId + '\'' +
                   '}';
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            SnapshotFile that = (SnapshotFile) object;
            return size == that.size
                   && dataDirectoryIndex == that.dataDirectoryIndex
                   && Objects.equals(name, that.name)
                   && Objects.equals(path, that.path)
                   && Objects.equals(tableId, that.tableId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, path, size, dataDirectoryIndex, tableId);
        }
    }

    private static <K, V> AbstractMap.SimpleEntry<K, V> pair(K key, V value)
    {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
