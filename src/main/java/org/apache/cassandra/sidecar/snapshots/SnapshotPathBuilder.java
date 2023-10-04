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
import java.util.function.BiPredicate;
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
import org.apache.cassandra.sidecar.data.SnapshotRequest;
import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.utils.BaseFileSystem;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.jetbrains.annotations.Nullable;

/**
 * This class builds the snapshot path on a given host validating that it exists
 */
@Singleton
public class SnapshotPathBuilder extends BaseFileSystem
{
    private static final String DATA_SUB_DIR = "/data";
    public static final int SNAPSHOTS_MAX_DEPTH = 5;
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
     */
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
     * Builds the path to the given snapshot directory given the {@code keyspace}, {@code table},
     * and {@code snapshotName} inside the specified {@code host}. When a table has been dropped and recreated,
     * the code searches for the latest modified directory for that table.
     *
     * @param host    the name of the host
     * @param request the request to list the snapshot files
     * @return the absolute path of the snapshot directory
     */
    public Future<String> build(String host, SnapshotRequest request)
    {
        return dataDirectories(host)
               .compose(dataDirs -> findKeyspaceDirectory(dataDirs, request.keyspace()))
               .compose(keyspaceDirectory -> findTableDirectory(keyspaceDirectory, request.tableName()))
               .compose(tableDirectory -> findSnapshotDirectory(tableDirectory, request.snapshotName()));
    }

    /**
     * Lists the snapshot directory, if {@code includeSecondaryIndexFiles} is true, the future
     * will include files inside secondary index directories.
     *
     * @param snapshotDirectory          the path to the snapshot directory
     * @param includeSecondaryIndexFiles whether to include secondary index files
     * @return a future with a list of files inside the snapshot directory
     */
    public Future<List<SnapshotFile>> listSnapshotDirectory(String snapshotDirectory,
                                                            boolean includeSecondaryIndexFiles)
    {
        Promise<List<SnapshotFile>> promise = Promise.promise();

        // List the snapshot directory
        fs.readDir(snapshotDirectory)
          .onFailure(promise::fail)
          .onSuccess(list -> {

              logger.debug("Found {} files in snapshot directory '{}'", list.size(), snapshotDirectory);

              // Prepare futures to get properties for all the files from listing the snapshot directory
              List<Future<FileProps>> futures = list.stream()
                                                    .map(fs::props)
                                                    .collect(Collectors.toList());

              Future.all(futures)
                    .onFailure(cause -> {
                        logger.debug("Failed to get FileProps", cause);
                        promise.fail(cause);
                    })
                    .onSuccess(ar -> {

                        // Create a pair of path/fileProps for every regular file
                        List<SnapshotFile> snapshotList =
                        IntStream.range(0, list.size())
                                 .filter(i -> ar.<FileProps>resultAt(i).isRegularFile())
                                 .mapToObj(i -> {
                                     long size = ar.<FileProps>resultAt(i).size();
                                     return new SnapshotFile(list.get(i),
                                                             size);
                                 })
                                 .collect(Collectors.toList());


                        if (!includeSecondaryIndexFiles)
                        {
                            // We are done if we don't include secondary index files
                            promise.complete(snapshotList);
                            return;
                        }

                        // Find index directories and prepare futures listing the snapshot directory
                        List<Future<List<SnapshotFile>>> idxListFutures =
                        IntStream.range(0, list.size())
                                 .filter(i -> {
                                     if (ar.<FileProps>resultAt(i).isDirectory())
                                     {
                                         Path path = Paths.get(list.get(i));
                                         int count = path.getNameCount();
                                         return count > 0
                                                && path.getName(count - 1)
                                                       .toString()
                                                       .startsWith(".");
                                     }
                                     return false;
                                 })
                                 .mapToObj(i -> listSnapshotDirectory(list.get(i), false))
                                 .collect(Collectors.toList());
                        if (idxListFutures.isEmpty())
                        {
                            // If there are no secondary index directories we are done
                            promise.complete(snapshotList);
                            return;
                        }
                        logger.debug("Found {} index directories in the '{}' snapshot",
                                     idxListFutures.size(), snapshotDirectory);
                        // if we have index directories, list them all
                        Future.all(idxListFutures)
                              .onFailure(promise::fail)
                              .onSuccess(idx -> {
                                  //noinspection unchecked
                                  List<SnapshotFile> idxPropList =
                                  idx.list()
                                     .stream()
                                     .flatMap(l -> ((List<SnapshotFile>) l).stream())
                                     .collect(Collectors.toList());

                                  // aggregate the results and return the full list
                                  snapshotList.addAll(idxPropList);
                                  promise.complete(snapshotList);
                              });
                    });
          });
        return promise.future();
    }

    /**
     * Finds the list of directories that match the given {@code snapshotName} inside the specified {@code host}.
     *
     * @param host         the name of the host
     * @param snapshotName the name of the snapshot
     * @return a list of absolute paths for the directories that match the given {@code snapshotName} inside the
     * specified {@code host}
     */
    public Future<List<String>> findSnapshotDirectories(String host, String snapshotName)
    {
        return dataDirectories(host)
               .compose(dataDirs -> findSnapshotDirectoriesRecursively(dataDirs, snapshotName));
    }

    /**
     * An optimized implementation to search snapshot directories by {@code snapshotName} recursively in all
     * the provided {@code dataDirs}.
     *
     * @param dataDirs     a list of data directories
     * @param snapshotName the name of the snapshot
     * @return a future with the list of snapshot directories that match the {@code snapshotName}
     */
    protected Future<List<String>> findSnapshotDirectoriesRecursively(List<String> dataDirs, String snapshotName)
    {
        Path snapshotsDirPath = Paths.get(SNAPSHOTS_DIR_NAME);
        Path snapshotNamePath = Paths.get(snapshotName);

        return executorPools.internal().executeBlocking(promise -> {
            // a filter to keep directories ending in "/snapshots/<snapshotName>"
            BiPredicate<Path, BasicFileAttributes> filter = (path, basicFileAttributes) -> {
                int nameCount;
                return basicFileAttributes.isDirectory() &&
                       (nameCount = path.getNameCount()) >= 2 &&
                       path.getName(nameCount - 2).endsWith(snapshotsDirPath) &&
                       path.getName(nameCount - 1).endsWith(snapshotNamePath);
            };

            // Using optimized Files.find instead of vertx's fs.readDir. Unfortunately
            // fs.readDir is limited, and it doesn't support recursively searching for files
            List<String> result = new ArrayList<>();
            for (String dataDir : dataDirs)
            {
                try (Stream<Path> directoryStream = Files.find(Paths.get(dataDir).toAbsolutePath(),
                                                               SNAPSHOTS_MAX_DEPTH, filter))
                {
                    result.addAll(directoryStream.map(Path::toString).collect(Collectors.toList()));
                }
                catch (IOException e)
                {
                    promise.fail(e);
                    return;
                }
            }

            promise.complete(result);
        });
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
            validator.validatePattern(secondaryIndexName.substring(1), "secondary index");
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
     * Constructs the path to the snapshot directory using the {@code baseDirectory} and {@code snapshotName}
     * and returns if it is a valid path to the snapshot directory, or a failure otherwise.
     *
     * @param baseDirectory the base directory where we search the snapshot directory
     * @param snapshotName  the name of the snapshot
     * @return a future for the path to the snapshot directory if it's valid, or a failed future otherwise
     */
    protected Future<String> findSnapshotDirectory(String baseDirectory, String snapshotName)
    {
        String snapshotDirectory = StringUtils.removeEnd(baseDirectory, File.separator) +
                                   File.separator + SNAPSHOTS_DIR_NAME + File.separator + snapshotName;

        return isValidDirectory(snapshotDirectory)
               .recover(t ->
                        {
                            String errMsg = String.format("Snapshot directory '%s' does not exist", snapshotName);
                            logger.warn("Snapshot directory {} does not exist in {}", snapshotName, baseDirectory);
                            return Future.failedFuture(new NoSuchFileException(errMsg));
                        });
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
                                                  .map(fs::props)
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

    /**
     * Class representing a snapshot component file
     */
    public static class SnapshotFile
    {
        public final String path;
        public final long size;

        SnapshotFile(String path, long size)
        {
            this.path = path;
            this.size = size;
        }
    }

    private static <K, V> AbstractMap.SimpleEntry<K, V> pair(K key, V value)
    {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
