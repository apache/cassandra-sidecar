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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cache.ToggleableCache;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.data.SnapshotRequest;
import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.jetbrains.annotations.VisibleForTesting;


/**
 * A {@link SnapshotPathBuilder} implementation that uses caches to optimize filesystem access patterns
 * while a bulk reader client is using Sidecar to stream SSTable components allowing for faster lookups.
 */
public class CachedSnapshotPathBuilder extends SnapshotPathBuilder
{
    /**
     * The table directory cache maintains a cache of recently used table directories. This cache
     * avoids having to traverse the filesystem searching for the table directory while running
     * bulk reads for example. Since bulk reads can stream hundreds of SSTables from the table directory,
     * this cache helps avoid having to resolve the table directory for every SSTable.
     */
    @VisibleForTesting
    final Cache<String, Future<String>> tableDirCache;

    /**
     * The snapshot list cache maintains a cache of recently listed snapshot files under a snapshot directory.
     * This cache avoids having to access the filesystem every time a bulk reader client lists the snapshot
     * directory.
     */
    @VisibleForTesting
    final Cache<String, Future<List<SnapshotFile>>> snapshotListCache;

    /**
     * The snapshot path cache maintains a cache of recently streamed snapshot SSTable components. This cache
     * avoids having to resolve the snapshot SSTable component file during bulk reads. Since bulk reads stream
     * sub-ranges of an SSTable component, the resolution can happen multiple times during bulk reads for a single
     * SSTable component. This cache aims to reduce filesystem access to resolve the SSTable component path
     * during bulk reads.
     */
    @VisibleForTesting
    final Cache<String, Future<String>> snapshotPathCache;

    /**
     * Constructs a new {@link CachedSnapshotPathBuilder} with the given {@code vertx}, {@code cassandraConfig},
     * and {@code cacheFactory}.
     *
     * @param vertx                the vertx instance
     * @param serviceConfiguration the configuration for running the Sidecar service
     * @param instancesConfig      the configuration for Cassandra
     * @param validator            a validator for performing Cassandra input validation
     * @param executorPools        executor pools for blocking executions
     */
    public CachedSnapshotPathBuilder(Vertx vertx,
                                     ServiceConfiguration serviceConfiguration,
                                     InstancesConfig instancesConfig,
                                     CassandraInputValidator validator,
                                     ExecutorPools executorPools)
    {
        super(vertx, instancesConfig, validator, executorPools);
        this.tableDirCache = initializeCache(serviceConfiguration.sstableSnapshotConfiguration()
                                                                 .tableDirCacheConfiguration(),
                                             "tableDirCache");
        this.snapshotListCache = initializeCache(serviceConfiguration.sstableSnapshotConfiguration()
                                                                     .snapshotListCacheConfiguration(),
                                                 "snapshotListCache");
        this.snapshotPathCache = initializeCache(serviceConfiguration.sstableSnapshotConfiguration()
                                                                     .snapshotPathCacheConfiguration(),
                                                 "snapshotPathCache");
    }

    /**
     * Builds the path to the component file on disk. Caches table directories in the provided
     * cache.
     *
     * @param host    the name of the host
     * @param request the request to stream the SSTable component
     * @return the path to the component file on disk
     */
    @Override
    public Future<String> build(String host, StreamSSTableComponentRequest request)
    {
        String key = host + ":" + request;
        return snapshotPathCache.get(key, f -> {
            // first thread wins
            validate(request);

            // Search for the file
            return dataDirectories(host)
                   .compose(dataDirs -> findTableDirectory(dataDirs, host, request.qualifiedTableName()))
                   .compose(tableDirectory -> findComponent(tableDirectory,
                                                            request.snapshotName(),
                                                            request.secondaryIndexName(),
                                                            request.componentName()));
        });
    }

    /**
     * Builds the path to the snapshot directory on disk. Caches table directories in the provided
     * cache.
     *
     * @param host    the name of the host
     * @param request the request to list the snapshot files
     * @return the absolute path of the snapshot directory
     */
    @Override
    public Future<String> build(String host, SnapshotRequest request)
    {
        // Search for the snapshot directory
        return dataDirectories(host)
               .compose(dataDirs -> findTableDirectory(dataDirs, host, request.qualifiedTableName()))
               .compose(tableDirectory -> findSnapshotDirectory(tableDirectory, request.snapshotName()));
    }

    /**
     * Makes sure that the directory exists on disk and returns the list of files inside the {@code snapshotDirectory},
     * including the secondary index files when {@code includeSecondaryIndexFiles} is {@code true}. Caches the
     * list of files, and returns the cached version of the snapshot only if the directory still exists.
     *
     * @param snapshotDirectory          the path to the snapshot directory
     * @param includeSecondaryIndexFiles whether to include secondary index files
     * @return a future with a list of files inside the snapshot directory
     */
    @Override
    public Future<List<SnapshotFile>> listSnapshotDirectory(String snapshotDirectory,
                                                            boolean includeSecondaryIndexFiles)
    {
        String key = snapshotDirectory + ":" + includeSecondaryIndexFiles;
        return snapshotListCache.get(key, f -> {
            // first thread wins
            return super.listSnapshotDirectory(snapshotDirectory, includeSecondaryIndexFiles);
        });
    }

    /**
     * Finds the table directory in the cache. Fails when the directory is not found
     *
     * @param dataDirs the data directories for this host
     * @param host     the name of the host
     * @param name     the Cassandra keyspace and table name
     * @return the directory path for the table
     */
    private Future<String> findTableDirectory(List<String> dataDirs, String host, QualifiedTableName name)
    {
        String key = host + ":" + name.keyspace() + ":" + name.tableName();
        return tableDirCache.get(key, f -> {
            // first thread wins
            return getTableDirectory(dataDirs, name);
        });
    }

    /**
     * Returns a future of the table directory if it exists, or a failed future if it doesn't
     *
     * @param dataDirs the data directories for this host
     * @param name     the Cassandra keyspace and table name
     * @return a future of the table directory if it exists, or a failed future if it doesn't
     */
    protected Future<String> getTableDirectory(List<String> dataDirs, QualifiedTableName name)
    {
        return findKeyspaceDirectory(dataDirs, name.keyspace())
               .compose(keyspaceDirectory -> super.findTableDirectory(keyspaceDirectory, name.tableName()));
    }

    protected <T> Cache<String, Future<T>> initializeCache(CacheConfiguration cacheConfiguration, String cacheName)
    {
        Cache<String, Future<T>> delegate =
        Caffeine.newBuilder()
                .maximumSize(cacheConfiguration.maximumSize())
                .expireAfterAccess(cacheConfiguration.expireAfterAccessMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .removalListener((key, value, cause) ->
                                 logger.debug("Removed from cache={}, entry={}, key={}, cause={}",
                                              cacheName, value, key, cause))
                .build();
        return new ToggleableCache<>(delegate, cacheConfiguration::enabled);
    }
}
