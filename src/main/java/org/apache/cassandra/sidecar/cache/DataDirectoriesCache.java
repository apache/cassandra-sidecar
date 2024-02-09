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

package org.apache.cassandra.sidecar.cache;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;

/**
 * A cache for data directories
 */
@Singleton
public class DataDirectoriesCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataDirectoriesCache.class);
    private final Cache<String, List<String>> cache;
    private final CacheConfiguration cacheConfiguration;
    private final InstanceMetadataFetcher metadataFetcher;

    @Inject
    public DataDirectoriesCache(ServiceConfiguration serviceConfiguration, InstanceMetadataFetcher metadataFetcher)
    {
        cacheConfiguration = serviceConfiguration.sstableSnapshotConfiguration()
                                                 .tableDirCacheConfiguration();
        this.metadataFetcher = metadataFetcher;
        if (cacheConfiguration != null)
        {
            cache = Caffeine.newBuilder()
                            .maximumSize(cacheConfiguration.maximumSize())
                            .expireAfterAccess(cacheConfiguration.expireAfterAccessMillis(), TimeUnit.MILLISECONDS)
                            .recordStats()
                            .removalListener((key, value, cause) ->
                                             LOGGER.debug("Removed from cache=data_directories, entry={}, key={}, " +
                                                          "cause={}", value, key, cause))
                            .build();
        }
        else
        {
            cache = null;
        }
    }

    public List<String> dataPaths(String host, String keyspace, String table) throws IOException
    {
        if (cache != null && cacheConfiguration.enabled())
        {
            String key = host + ":" + keyspace + ":" + table;
            return cache.get(key, k -> dataPathsInternal(host, keyspace, table));
        }
        return dataPathsInternal(host, keyspace, table);
    }

    private List<String> dataPathsInternal(String host, String keyspace, String table)
    {
        CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
        if (delegate == null)
        {
            throw cassandraServiceUnavailable();
        }

        TableOperations tableOperations = delegate.tableOperations();
        if (tableOperations == null)
        {
            throw cassandraServiceUnavailable();
        }

        try
        {
            return tableOperations.getDataPaths(keyspace, table);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
