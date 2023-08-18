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

package org.apache.cassandra.sidecar.utils;

import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;

/**
 * Provides functionality for filesystem operations
 */
public class BaseFileSystem
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final ExecutorPools executorPools;
    protected final FileSystem fs;
    protected final InstancesConfig instancesConfig;
    protected final CassandraInputValidator validator;

    /**
     * Creates a new FileSystemUtils with the given {@code vertx} instance.
     *
     * @param fileSystem      async file system abstraction from vertx
     * @param instancesConfig the configuration for Cassandra
     * @param validator       validates cassandra related inputs
     * @param executorPools   executor pools for blocking executions
     */
    public BaseFileSystem(FileSystem fileSystem,
                          InstancesConfig instancesConfig,
                          CassandraInputValidator validator,
                          ExecutorPools executorPools)
    {
        this.fs = fileSystem;
        this.instancesConfig = instancesConfig;
        this.validator = validator;
        this.executorPools = executorPools;
    }

    /**
     * @param host the host
     * @return the data directories for the given {@code host}
     */
    protected Future<List<String>> dataDirectories(String host)
    {
        List<String> dataDirs = instancesConfig.instanceFromHost(host).dataDirs();
        if (dataDirs == null || dataDirs.isEmpty())
        {
            String errMsg = String.format("No data directories are available for host '%s'", host);
            logger.error(errMsg);
            return Future.failedFuture(new NoSuchFileException(errMsg));
        }
        return Future.succeededFuture(dataDirs);
    }

    /**
     * @param filename the path to the file
     * @return a future of the {@code filename} if it exists and is a regular file, a failed future otherwise
     */
    public Future<String> isValidFilename(String filename)
    {
        return isValidOfType(filename, FileProps::isRegularFile);
    }

    /**
     * @param path the path to the directory
     * @return a future of the {@code path} if it exists and is a directory, a failed future otherwise
     */
    public Future<String> isValidDirectory(String path)
    {
        return isValidOfType(path, FileProps::isDirectory);
    }

    /**
     * Creates the directory if it doesn't exist, and then validates that {@code path} is a valid directory.
     *
     * @param path the path to the directory
     * @return a future of the validated {@code path}, a failed future otherwise
     */
    public Future<String> ensureDirectoryExists(String path)
    {
        return fs.mkdirs(path).compose(v -> Future.succeededFuture(path));
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
                 .compose(exists -> {
                     if (!exists)
                     {
                         String errMsg = "File '" + filename + "' does not exist";
                         return Future.failedFuture(new NoSuchFileException(errMsg));
                     }
                     return fs.props(filename)
                              .compose(fileProps -> {
                                  if (fileProps == null || !predicate.test(fileProps))
                                  {
                                      String errMsg = "File '" + filename + "' does not exist";
                                      return Future.failedFuture(new NoSuchFileException(errMsg));
                                  }
                                  return Future.succeededFuture(filename);
                              });
                 });
    }
}
