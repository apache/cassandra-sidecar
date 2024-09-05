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

package org.apache.cassandra.sidecar.routes.snapshots;

import java.io.FileNotFoundException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.response.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.server.TableOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.metrics.CacheStatsCounter;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.data.SnapshotRequestParam;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.RequestUtils;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * The <b>GET</b> verb will produce a list of paths of all the snapshot files of a given
 * snapshot name.
 *
 * <p>The query param {@code includeSecondaryIndexFiles} is used to request secondary index
 * files along with other files. For example:
 *
 * <p>{@code /api/v1/keyspaces/ks/tables/tbl/snapshots/testSnapshot}
 * lists all SSTable component files for the <i>"testSnapshot"</i> snapshot for the
 * <i>"ks"</i> keyspace and the <i>"tbl"</i> table
 *
 * <p>{@code /api/v1/keyspaces/ks/tables/tbl/snapshots/testSnapshot?includeSecondaryIndexFiles=true}
 * lists all SSTable component files, including secondary index files, for the
 * <i>"testSnapshot"</i> snapshot for the <i>"ks"</i> keyspace and the <i>"tbl"</i> table
 */
@Singleton
public class ListSnapshotHandler extends AbstractHandler<SnapshotRequestParam>
{
    private static final String INCLUDE_SECONDARY_INDEX_FILES_QUERY_PARAM = "includeSecondaryIndexFiles";
    public static final String SNAPSHOT_CACHE_NAME = "snapshot_cache";
    private final SnapshotPathBuilder builder;
    private final ServiceConfiguration configuration;
    private final CacheConfiguration cacheConfiguration;
    private final Cache<String, Future<ListSnapshotFilesResponse>> cache;

    @Inject
    public ListSnapshotHandler(SnapshotPathBuilder builder,
                               ServiceConfiguration configuration,
                               InstanceMetadataFetcher metadataFetcher,
                               CassandraInputValidator validator,
                               ExecutorPools executorPools,
                               SidecarMetrics sidecarMetrics)
    {
        super(metadataFetcher, executorPools, validator);
        this.builder = builder;
        this.configuration = configuration;
        this.cacheConfiguration = configuration.sstableSnapshotConfiguration().snapshotListCacheConfiguration();
        this.cache = initializeCache(cacheConfiguration, sidecarMetrics.server().cacheMetrics().snapshotCacheMetrics);
    }

    /**
     * Lists paths of all the snapshot files of a given snapshot name.
     * <p>
     * The query param {@code includeSecondaryIndexFiles} is used to request secondary index
     * files along with other files. For example:
     * <p>
     * {@code /api/v1/keyspaces/ks/tables/tbl/snapshots/testSnapshot}
     * lists all SSTable component files for the <i>"testSnapshot"</i> snapshot for the
     * <i>"ks"</i> keyspace and the <i>"tbl"</i> table
     * <p>
     * {@code /api/v1/keyspaces/ks/tables/tbl/snapshots/testSnapshot?includeSecondaryIndexFiles=true}
     * lists all SSTable component files, including secondary index files, for the
     * <i>"testSnapshot"</i> snapshot for the <i>"ks"</i> keyspace and the <i>"tbl"</i> table
     *
     * @param context       the event to handle
     * @param httpRequest   the {@link HttpServerRequest} object
     * @param host          the name of the host
     * @param remoteAddress the remote address that originated the request
     * @param request       parameters obtained from the request
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               SnapshotRequestParam request)
    {
        cachedResponseOrProcess(host, request)
        .onSuccess(response -> {
            if (response.snapshotFilesInfo().isEmpty())
            {
                String payload = "Snapshot '" + request.snapshotName() + "' not found";
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, payload));
            }
            else
            {
                logger.debug("SnapshotsHandler handled request={}, remoteAddress={}, " +
                             "instance={}", request, remoteAddress, host);
                context.json(response);
            }
        })
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  SnapshotRequestParam request)
    {
        logger.error("SnapshotsHandler failed for request={}, remoteAddress={}, instance={}, method={}",
                     request, remoteAddress, host, context.request().method(), cause);
        if (cause instanceof FileNotFoundException || cause instanceof NoSuchFileException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
        }
        else
        {
            super.processFailure(cause, context, host, remoteAddress, request);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SnapshotRequestParam extractParamsOrThrow(RoutingContext context)
    {
        boolean includeSecondaryIndexFiles =
        RequestUtils.parseBooleanQueryParam(context.request(), INCLUDE_SECONDARY_INDEX_FILES_QUERY_PARAM, false);

        return SnapshotRequestParam.builder()
                                   .qualifiedTableName(qualifiedTableName(context))
                                   .snapshotName(context.pathParam("snapshot"))
                                   .includeSecondaryIndexFiles(includeSecondaryIndexFiles)
                                   .build();
    }

    protected Future<ListSnapshotFilesResponse> cachedResponseOrProcess(String host, SnapshotRequestParam request)
    {
        if (cache != null && cacheConfiguration.enabled())
        {
            String key = host + ":" + request;
            return cache.get(key, k -> processResponse(host, request));
        }
        return processResponse(host, request);
    }

    protected Future<ListSnapshotFilesResponse> processResponse(String host, SnapshotRequestParam request)
    {
        return dataPaths(host, request.keyspace(), request.tableName())
               .compose(dataDirectoryList ->
                        builder.streamSnapshotFiles(dataDirectoryList,
                                                    request.snapshotName(),
                                                    request.includeSecondaryIndexFiles())
               )
               .compose(snapshotFileStream -> buildResponse(host, request, snapshotFileStream));
    }

    protected Future<List<String>> dataPaths(String host, String keyspace, String table)
    {
        return executorPools.service().executeBlocking(() -> {
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

            return tableOperations.getDataPaths(keyspace, table);
        });
    }

    protected Future<ListSnapshotFilesResponse>
    buildResponse(String host,
                  SnapshotRequestParam request,
                  Stream<SnapshotPathBuilder.SnapshotFile> snapshotFileStream)
    {
        int sidecarPort = configuration.port();
        ListSnapshotFilesResponse response = new ListSnapshotFilesResponse();
        snapshotFileStream.forEach(file -> {
            String tableNameAndId = file.tableId != null
                                    ? request.tableName() + "-" + file.tableId
                                    : request.tableName();
            response.addSnapshotFile(new ListSnapshotFilesResponse.FileInfo(file.size,
                                                                            host,
                                                                            sidecarPort,
                                                                            file.dataDirectoryIndex,
                                                                            request.snapshotName(),
                                                                            request.keyspace(),
                                                                            tableNameAndId,
                                                                            file.name));
        });
        return Future.succeededFuture(response);
    }

    protected Cache<String, Future<ListSnapshotFilesResponse>> initializeCache(CacheConfiguration cacheConfiguration,
                                                                               CacheStatsCounter snapshotCacheMetrics)
    {
        if (cacheConfiguration == null)
        {
            return null;
        }
        return Caffeine.newBuilder()
                       .maximumSize(cacheConfiguration.maximumSize())
                       .expireAfterAccess(cacheConfiguration.expireAfterAccessMillis(), TimeUnit.MILLISECONDS)
                       .recordStats(() -> snapshotCacheMetrics)
                       .removalListener((key, value, cause) ->
                                        logger.debug("Removed from cache={}, entry={}, key={}, cause={}",
                                                     SNAPSHOT_CACHE_NAME, value, key, cause))
                       .build();
    }
}
