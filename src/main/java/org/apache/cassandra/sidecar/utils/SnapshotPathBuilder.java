package org.apache.cassandra.sidecar.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.common.utils.ValidationUtils;

/**
 * This class builds the snapshot path on a given host validating that it exists
 */
@Singleton
public class SnapshotPathBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotPathBuilder.class);
    private static final String DATA_SUB_DIR = "/data";
    public static final String SNAPSHOTS_DIR_NAME = "snapshots";
    protected final FileSystem fs;
    protected final InstancesConfig instancesConfig;

    /**
     * Creates a new SnapshotPathBuilder instance with the given {@code fs filesystem} and
     * {@code instancesConfig Cassandra configuration}.
     *
     * @param fs              the underlying filesystem
     * @param instancesConfig the configuration for Cassandra
     */
    @Inject
    public SnapshotPathBuilder(FileSystem fs, InstancesConfig instancesConfig)
    {
        this.fs = fs;
        this.instancesConfig = instancesConfig;
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
        return getDataDirectories(host)
               .compose(dataDirs -> findKeyspaceDirectory(dataDirs, request.getKeyspace()))
               .compose(keyspaceDirectory -> findTableDirectory(keyspaceDirectory, request.getTableName()))
               .compose(tableDirectory -> findComponent(tableDirectory, request.getSnapshotName(),
                                                        request.getComponentName()));
    }

    /**
     * Validates that the component name is either {@code *.db} or a {@code *-TOC.txt}
     * which are the only required components to read SSTables.
     *
     * @param request the request to stream the SSTable component
     */
    protected void validate(StreamSSTableComponentRequest request)
    {
        // Only allow .db and TOC.txt components here
        ValidationUtils.validateDbOrTOCComponentName(request.getComponentName());
    }

    /**
     * @param host the host
     * @return the data directories for the given {@code host}
     */
    protected Future<List<String>> getDataDirectories(String host)
    {
        List<String> dataDirs = instancesConfig.instanceFromHost(host).dataDirs();
        if (dataDirs == null || dataDirs.isEmpty())
        {
            logger.error("No data directories are available for host '{}'", host);
            String errMsg = String.format("No data directories are available for host '%s'", host);
            return Future.failedFuture(new FileNotFoundException(errMsg));
        }
        return Future.succeededFuture(dataDirs);
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
        String errMsg = String.format("Keyspace '%s' does not exist", keyspace);
        return root.recover(t -> Future.failedFuture(new FileNotFoundException(errMsg)));
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
                 .compose(list -> getLastModifiedTableDirectory(list, tableName));
    }

    /**
     * Constructs the path to the component using the {@code baseDirectory}, {@code snapshotName}, and
     * {@code componentName} and returns if it is a valid path to the component, or a failure otherwise.
     *
     * @param baseDirectory the base directory where we search the table directory
     * @param snapshotName  the name of the snapshot
     * @param componentName the name of the component
     * @return the path to the component if it's valid, a failure otherwise
     */
    protected Future<String> findComponent(String baseDirectory, String snapshotName, String componentName)
    {
        String componentFilename = StringUtils.removeEnd(baseDirectory, File.separator) +
                                   File.separator + SNAPSHOTS_DIR_NAME + File.separator + snapshotName +
                                   File.separator + componentName;

        return isValidFilename(componentFilename)
               .recover(t ->
               {
                   logger.warn("Snapshot directory {} or component {} does not exist in {}", snapshotName,
                               componentName, componentFilename);
                   String errMsg = String.format("Component '%s' does not exist for snapshot '%s'",
                                                 componentName, snapshotName);
                   return Future.failedFuture(new FileNotFoundException(errMsg));
               });
    }

    /**
     * @param filename the path to the file
     * @return a future of the {@code filename} if it exists and is a regular file, a failed future otherwise
     */
    protected Future<String> isValidFilename(String filename)
    {
        return isValidOfType(filename, FileProps::isRegularFile);
    }

    /**
     * @param path the path to the directory
     * @return a future of the {@code path} if it exists and is a directory, a failed future otherwise
     */
    protected Future<String> isValidDirectory(String path)
    {
        return isValidOfType(path, FileProps::isDirectory);
    }

    /**
     * @param filename  the path
     * @param predicate a predicate that evaluates based on {@link FileProps}
     * @return a future of the {@code filename} if it exists and {@code predicate} evaluates to true,
     * a failed future otherwise
     */
    protected Future<String> isValidOfType(String filename, Predicate<FileProps> predicate)
    {
        return fs.exists(filename)
                 .compose(exists ->
                 {
                     if (!exists)
                     {
                         String errMsg = "File '" + filename + "' does not exist";
                         return Future.failedFuture(new FileNotFoundException(errMsg));
                     }
                     return fs.props(filename)
                              .compose(fileProps ->
                              {
                                  if (fileProps == null || !predicate.test(fileProps))
                                  {
                                      String errMsg = "File '" + filename + "' does not exist";
                                      return Future.failedFuture(new FileNotFoundException(errMsg));
                                  }
                                  return Future.succeededFuture(filename);
                              });
                 });
    }

    /**
     * @param fileList  a list of files
     * @param tableName the name of the Cassandra table
     * @return a future with the last modified directory from the list, or a failed future when there are no directories
     */
    protected Future<String> getLastModifiedTableDirectory(List<String> fileList, String tableName)
    {
        if (fileList.size() == 0)
        {
            String errMsg = String.format("Table '%s' does not exist", tableName);
            return Future.failedFuture(new FileNotFoundException(errMsg));
        }

        //noinspection rawtypes
        List<Future> futures = fileList.stream()
                                       .map(fs::props)
                                       .collect(Collectors.toList());

        Promise<String> promise = Promise.promise();
        CompositeFuture.all(futures)
                       .onFailure(promise::fail)
                       .onSuccess(ar ->
                       {
                           String directory = IntStream.range(0, fileList.size())
                                                       .mapToObj(i -> Pair.of(fileList.get(i),
                                                                              ar.<FileProps>resultAt(i)))
                                                       .filter(pair -> pair.getRight().isDirectory())
                                                       .max(Comparator.comparingLong(pair -> pair.getRight()
                                                                                                 .lastModifiedTime()))
                                                       .map(Pair::getLeft)
                                                       .orElse(null);

                           if (directory == null)
                           {
                               String errMsg = String.format("Table '%s' does not exist", tableName);
                               promise.fail(new FileNotFoundException(errMsg));
                           }
                           else
                           {
                               promise.complete(directory);
                           }
                       });
        return promise.future();
    }
}
