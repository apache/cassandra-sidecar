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

package org.apache.cassandra.sidecar.handlers.sysinfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.data.DiskInfo;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;
import oshi.SystemInfo;
import oshi.software.os.OSFileStore;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler that retrieves disk information for all file stores on the system.
 * Uses OSHI library to gather metrics about storage capacity and usage for each mounted file system.
 * Disk information is cached for 5 minutes to avoid expensive OSHI calls on every request.
 */
@Singleton
public class DiskInfoHandler extends AbstractHandler<Void> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DiskInfoHandler.class);
    private static final String CACHE_KEY = "disk_info";
    private static final Duration CACHE_TTL = Duration.ofMinutes(5);

    private final LoadingCache<String, List<DiskInfo>> diskInfoCache;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected DiskInfoHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools, CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);

        this.diskInfoCache = Caffeine.newBuilder()
                                     .expireAfterWrite(CACHE_TTL)
                                     .maximumSize(1)
                                     .build(new DiskInfoCacheLoader());
    }

    /**
     * Cache loader that fetches disk information from the system using OSHI
     */
    private static class DiskInfoCacheLoader implements CacheLoader<String, List<DiskInfo>>
    {
        @Override
        public List<DiskInfo> load(@NotNull String key)
        {
            SystemInfo systemInfo = new SystemInfo();
            List<OSFileStore> fileStoreList = systemInfo.getOperatingSystem().getFileSystem().getFileStores();
            List<DiskInfo> diskInfoList = new ArrayList<>(fileStoreList.size());

            for (OSFileStore fileStore : fileStoreList)
            {
                diskInfoList.add(new DiskInfo(fileStore.getTotalSpace(),
                                              fileStore.getFreeSpace(),
                                              fileStore.getUsableSpace(),
                                              fileStore.getName(),
                                              fileStore.getMount(),
                                              fileStore.getType()));
            }

            return diskInfoList;
        }
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        // No params to extract
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  Void request)
    {
        executorPools.internal()
                     .executeBlocking(() -> diskInfoCache.get(CACHE_KEY))
                     .onSuccess(context::json)
                     .onFailure(e -> {
                         LOGGER.warn("Failed to fetch disk information", e);
                         context.fail(wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                                       e.getMessage(), e));
                     });
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.SYSTEM.toAuthorization());
    }
}
