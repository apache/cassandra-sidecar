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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.exceptions.InsufficientStorageException;

/**
 * A collection of useful async API extensions of Vertx FileSystem
 */
public class AsyncFileSystemUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFileSystemUtils.class);

    private AsyncFileSystemUtils()
    {
        throw new UnsupportedOperationException("Cannot instantiate utility class");
    }

    /**
     * Asynchronously read the props of the {@link FileStore} located by the path
     * @param path path to locate its belonging {@link FileStore}
     * @param executorPool thread pool to run the operation
     * @return future of the read
     */
    public static Future<FileStoreProps> fileStoreProps(String path, TaskExecutorPool executorPool)
    {
        return executorPool.executeBlocking(promise -> {
           try
           {
               // Navigate to the parent paths until finding an existing one
               // The input path might not exist yet.
               // In the worst case, the currentPath is at the file system root, and it must be non-null.
               Path currentPath = Paths.get(path);
               while (currentPath != null && !Files.exists(currentPath))
               {
                   currentPath = currentPath.getParent();
               }
               FileStore fs = Files.getFileStore(currentPath);
               promise.tryComplete(new FileStoreProps(fs.name(),
                                                      fs.getTotalSpace(),
                                                      fs.getUsableSpace(),
                                                      fs.getUnallocatedSpace()));
           }
           catch (IOException e)
           {
               promise.tryFail(new RuntimeException("Failed to read the belonging file store of path: " + path, e));
           }
        });
    }

    /**
     * Asynchronously check whether the file store can satisfy the required unusable space percentage.
     * The check fails when the current usable space falls below the required usable space percentage.
     * @param path path to locate its belonging {@link FileStore}
     * @param requiredUsablePercentage required usable space percentage
     * @param executorPool thread pool to run the operation
     * @return future of the check
     */
    public static Future<Void> ensureSufficientStorage(String path,
                                                       double requiredUsablePercentage,
                                                       TaskExecutorPool executorPool)
    {
        return ensureSufficientStorage(path, 0, requiredUsablePercentage, executorPool);
    }

    /**
     * Asynchronously check whether the file store can satisfy both the requested space and the required
     * unusable space percentage. If any of the condition fails, the check fails.
     * @param path path to locate its belonging {@link FileStore}
     * @param requestedSpace requested space to use, in bytes
     * @param requiredUsablePercentage required usable space percentage
     * @param executorPool thread pool to run the operation
     * @return future of the check
     */
    public static Future<Void> ensureSufficientStorage(String path,
                                                       long requestedSpace,
                                                       double requiredUsablePercentage,
                                                       TaskExecutorPool executorPool)
    {
        return fileStoreProps(path, executorPool)
               .map(props -> { // A confidence check. It should not happen.
                   if (props.usableSpace > props.totalSpace)
                   {
                       LOGGER.error("Invalid file store state. fileStore={} path={} available={} total={}",
                                    props.name, path, props.usableSpace, props.totalSpace);
                       throw new IllegalStateException("FileStore has less total space than usable space. " +
                                                       "FileStore: " + props.name +
                                                       ", total space: " + props.totalSpace
                                                       + ", usable space: " + props.usableSpace);
                   }
                   return props;
               })
               .compose(props -> {
                   long deductedUsableSpace = props.usableSpace - requestedSpace;
                   long requiredUsableSpace = (long) Math.ceil(props.totalSpace * requiredUsablePercentage);
                   if (deductedUsableSpace < requiredUsableSpace)
                   {
                       LOGGER.error("Insufficient space available. " +
                                    "fileStore={} path={} available={} requested={} required={}",
                                    props.name, path, props.usableSpace, requestedSpace, requiredUsableSpace);
                       return Future.failedFuture(new InsufficientStorageException(props.name,
                                                                                   path,
                                                                                   props.totalSpace,
                                                                                   props.usableSpace,
                                                                                   requiredUsableSpace));
                   }
                   return Future.succeededFuture();
               });
    }

    /**
     * Properties of a {@link FileStore}
     */
    public static class FileStoreProps
    {
        // the name of this file store
        public final String name;
        // the size, in bytes, of the file store
        public final long totalSpace;
        // the number of bytes available to this Java virtual machine on the file store
        public final long usableSpace;
        // the number of unallocated bytes in the file store
        public final long unallocatedSpace;

        public FileStoreProps(String name, long totalSpace, long usableSpace, long unallocatedSpace)
        {
            this.name = name;
            this.totalSpace = totalSpace;
            this.usableSpace = usableSpace;
            this.unallocatedSpace = unallocatedSpace;
        }
    }
}
