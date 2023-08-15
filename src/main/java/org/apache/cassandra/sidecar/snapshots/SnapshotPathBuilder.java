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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileProps;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.utils.BaseFileSystem;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * This class builds the snapshot path on a given host validating that it exists
 */
@Singleton
public class SnapshotPathBuilder extends BaseFileSystem
{
    private static final String DATA_SUB_DIR = "/data";
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
     * Builds the path to the given component given the {@code keyspace}, {@code table}, and {@code snapshot}
     * inside the specified {@code host}. When a table has been dropped and recreated, the code searches for
     * the latest modified directory for that table.
     *
     * @param host    the name of the host
     * @param request the request to stream the SSTable component
     * @return the absolute path of the component
     * @deprecated this method is deprecated and should be removed when we stop supporting legacy
     * {@link StreamSSTableComponentHandler}'s streaming
     */
    @Deprecated
    public Future<String> build(String host, StreamSSTableComponentRequest request)
    {
        validate(request);
        // Search for the file
        return dataDirectories(host)
               .compose(dataDirs -> findKeyspaceDirectory(dataDirs, request.keyspace()))
               .compose(keyspaceDirectory -> findTableDirectory(keyspaceDirectory, request.tableName()))
               .compose(tableDirectory -> findComponent(tableDirectory,
                                                        request.snapshotName(),
                                                        request.secondaryIndexName(),
                                                        request.componentName()));
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
        return executorPools.internal().executeBlocking(blockingPromise -> {
            try
            {
                Stream<SnapshotFile> result =
                IntStream.range(0, tableDataDirectoryList.size())
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

                blockingPromise.complete(result);
            }
            catch (Throwable throwable)
            {
                blockingPromise.tryFail(throwable);
            }
        });
    }

    protected Stream<SnapshotFile> listSnapshotDir(int dataDirectoryIndex, Path snapshotDir,
                                                   boolean includeSecondaryIndexFiles)
    {
        String tableUuid = tableUuid(snapshotDir);
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
                                return new SnapshotFile(snapshotFileName, snapshotFile, attrs.size(), dataDirectoryIndex, tableUuid);
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
     * Searches in the list of {@code daraDirs} for the given {@code keyspace} and returns the directory
     * of the keyspace when it is found, or failure when the {@code keyspace} directory does not exist. If
     * one of the data directories does not exist, a failure will be reported.
     *
     * @param dataDirs the list of data directories for a given host
     * @param keyspace the name of the Cassandra keyspace
     * @return the directory of the keyspace when it is found, or failure if not found
     */
    protected Future<String> findKeyspaceDirectory(List<String> dataDirs, String keyspace)
    {
        List<Future<String>> candidates = buildPotentialKeyspaceDirectoryList(dataDirs, keyspace);
        // We want to find the first valid directory in this case. If a future fails, we
        // recover by checking each candidate for existence.
        // Whenever the first successful future returns, we short-circuit the rest.
        Future<String> root = candidates.get(0);
        for (int i = 1; i < candidates.size(); i++)
        {
            Future<String> f = candidates.get(i);
            root = root.recover(v -> f);
        }
        return root.recover(t -> {
            String errorMessage = String.format("Keyspace '%s' does not exist", keyspace);
            logger.debug(errorMessage, t);
            return Future.failedFuture(new NoSuchFileException(errorMessage));
        });
    }

    /**
     * Builds a list of potential directory lists for the keyspace
     *
     * @param dataDirs the list of directories
     * @param keyspace the Cassandra keyspace
     * @return a list of potential directories for the keyspace
     */
    private List<Future<String>> buildPotentialKeyspaceDirectoryList(List<String> dataDirs, String keyspace)
    {
        List<Future<String>> candidates = new ArrayList<>(dataDirs.size() * 2);
        for (String baseDirectory : dataDirs)
        {
            String dir = StringUtils.removeEnd(baseDirectory, File.separator);
            candidates.add(isValidDirectory(dir + File.separator + keyspace));
            candidates.add(isValidDirectory(dir + DATA_SUB_DIR + File.separator + keyspace));
        }
        return candidates;
    }

    /**
     * Finds the most recent directory for the given {@code tableName} in the {@code baseDirectory}. Cassandra
     * appends the table UUID when a table is created. When a table is dropped and then recreated, a new directory
     * with the new table UUID is created. For that reason we need to return the most recent directory for the
     * given table name.
     *
     * @param baseDirectory the base directory where we search the table directory
     * @param tableName     the name of the table
     * @return the most recent directory for the given {@code tableName} in the {@code baseDirectory}
     */
    protected Future<String> findTableDirectory(String baseDirectory, String tableName)
    {
        return fs.readDir(baseDirectory, tableName + "($|-.*)") // match exact table name or table-.*
                 .compose(list -> lastModifiedTableDirectory(list, tableName));
    }

    /**
     * Constructs the path to the component using the {@code baseDirectory}, {@code snapshotName}, and
     * {@code componentName} and returns if it is a valid path to the component, or a failure otherwise.
     *
     * @param baseDirectory      the base directory where we search the table directory
     * @param snapshotName       the name of the snapshot
     * @param secondaryIndexName the name of the secondary index (if provided)
     * @param componentName      the name of the component
     * @return the path to the component if it's valid, a failure otherwise
     */
    protected Future<String> findComponent(String baseDirectory, String snapshotName,
                                           @Nullable String secondaryIndexName, String componentName)
    {
        StringBuilder sb = new StringBuilder(StringUtils.removeEnd(baseDirectory, File.separator))
                           .append(File.separator).append(SNAPSHOTS_DIR_NAME)
                           .append(File.separator).append(snapshotName);
        if (secondaryIndexName != null)
        {
            sb.append(File.separator).append(secondaryIndexName);
        }
        String componentFilename = sb.append(File.separator).append(componentName).toString();

        return isValidFilename(componentFilename)
               .recover(t -> {
                   logger.warn("Snapshot directory {} or component {} does not exist in {}", snapshotName,
                               componentName, componentFilename);
                   String errMsg = String.format("Component '%s' does not exist for snapshot '%s'",
                                                 componentName, snapshotName);
                   return Future.failedFuture(new NoSuchFileException(errMsg));
               });
    }

    @NotNull
    public String resolveComponentPath(String baseDirectory,
                                       StreamSSTableComponentRequest request)
    {
        validate(request);
        StringBuilder sb = new StringBuilder(StringUtils.removeEnd(baseDirectory, File.separator))
                           .append(File.separator).append(request.keyspace())
                           .append(File.separator).append(request.tableName());
        if (request.tableUuid() != null)
        {
            sb.append("-").append(request.tableUuid());
        }
        sb.append(File.separator).append(SNAPSHOTS_DIR_NAME)
          .append(File.separator).append(request.snapshotName());
        if (request.secondaryIndexName() != null)
        {
            sb.append(File.separator).append(request.secondaryIndexName());
        }
        return sb.append(File.separator).append(request.componentName()).toString();
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
     * @param fileList  a list of files
     * @param tableName the name of the Cassandra table
     * @return a future with the last modified directory from the list, or a failed future when there are no directories
     */
    protected Future<String> lastModifiedTableDirectory(List<String> fileList, String tableName)
    {
        if (fileList.isEmpty())
        {
            String errMsg = String.format("Table '%s' does not exist", tableName);
            return Future.failedFuture(new NoSuchFileException(errMsg));
        }

        List<Future<FileProps>> futures = fileList.stream()
                                                  .map(filePropsProvider())
                                                  .collect(Collectors.toList());

        Promise<String> promise = Promise.promise();
        Future.all(futures)
              .onFailure(promise::fail)
              .onSuccess(ar -> {
                  String directory = IntStream.range(0, fileList.size())
                                              .mapToObj(i -> pair(fileList.get(i), ar.<FileProps>resultAt(i)))
                                              .filter(pair -> pair.getValue().isDirectory())
                                              .max(Comparator.comparingLong(pair -> pair.getValue()
                                                                                        .lastModifiedTime()))
                                              .map(AbstractMap.SimpleEntry::getKey)
                                              .orElse(null);

                  if (directory == null)
                  {
                      String errMsg = String.format("Table '%s' does not exist", tableName);
                      promise.fail(new NoSuchFileException(errMsg));
                  }
                  else
                  {
                      promise.complete(directory);
                  }
              });
        return promise.future();
    }

    private String tableUuid(@NotNull Path snapshotDir)
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

    @VisibleForTesting
    protected @NotNull Function<String, Future<FileProps>> filePropsProvider()
    {
        return fs::props;
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
        public final String tableUuid;

        @VisibleForTesting
        SnapshotFile(Path path, long size, int dataDirectoryIndex, String tableUuid)
        {
            this(Objects.requireNonNull(path.getFileName(), "path.getFileName() cannot be null").toString(),
                 path, size, dataDirectoryIndex, tableUuid);
        }

        SnapshotFile(String name, Path path, long size, int dataDirectoryIndex, String tableUuid)
        {
            this.name = name;
            this.path = path;
            this.size = size;
            this.dataDirectoryIndex = dataDirectoryIndex;
            this.tableUuid = tableUuid;
        }

        @Override
        public String toString()
        {
            return "SnapshotFile{" +
                   "name=" + name +
                   ", path=" + path +
                   ", size=" + size +
                   ", dataDirectoryIndex=" + dataDirectoryIndex +
                   ", tableUuid='" + tableUuid + '\'' +
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
                   && Objects.equals(tableUuid, that.tableUuid);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, path, size, dataDirectoryIndex, tableUuid);
        }
    }

    private static <K, V> AbstractMap.SimpleEntry<K, V> pair(K key, V value)
    {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
