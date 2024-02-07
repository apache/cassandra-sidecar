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
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

import com.google.common.base.Preconditions;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.data.SnapshotRequest;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.snapshots.SnapshotDirectory;
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
public class ListSnapshotHandler extends AbstractHandler<SnapshotRequest>
{
    private static final String INCLUDE_SECONDARY_INDEX_FILES_QUERY_PARAM = "includeSecondaryIndexFiles";
    private final SnapshotPathBuilder builder;
    private final ServiceConfiguration configuration;

    @Inject
    public ListSnapshotHandler(SnapshotPathBuilder builder,
                               ServiceConfiguration configuration,
                               InstanceMetadataFetcher metadataFetcher,
                               CassandraInputValidator validator,
                               ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, validator);
        this.builder = builder;
        this.configuration = configuration;
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
     * @param requestParams parameters obtained from the request
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               SnapshotRequest requestParams)
    {
        executorPools.service()
                     .executeBlocking(promise -> {
                         CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
                         if (delegate == null)
                         {
                             promise.fail(cassandraServiceUnavailable());
                             return;
                         }

                         TableOperations tableOperations = delegate.tableOperations();
                         if (tableOperations == null)
                         {
                             promise.fail(cassandraServiceUnavailable());
                             return;
                         }

                         try
                         {
                             promise.complete(tableOperations.getDataPaths(requestParams.keyspace(),
                                                                           requestParams.tableName()));
                         }
                         catch (IOException e)
                         {
                             String payload = String.format("Unable to retrieve data paths for %s.%s",
                                                            requestParams.keyspace(), requestParams.tableName());
                             promise.fail(wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, payload, e));
                         }
                     })
                     .compose(dataPathList ->
                              builder.listSnapshot(dataPathList,
                                                   requestParams.snapshotName(),
                                                   requestParams.includeSecondaryIndexFiles())
                     )
                     .onSuccess(fileList -> {
                         if (fileList.isEmpty())
                         {
                             String payload = "Snapshot '" + requestParams.snapshotName() + "' not found";
                             context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, payload));
                         }
                         else
                         {
                             logger.debug("SnapshotsHandler handled request={}, remoteAddress={}, " +
                                          "instance={}", requestParams, remoteAddress, host);
                             context.json(buildResponse(host, snapshotDirectory, fileList));
                         }
                     })
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, requestParams));
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  SnapshotRequest requestParams)
    {
        logger.error("SnapshotsHandler failed for request={}, remoteAddress={}, instance={}, method={}",
                     requestParams, remoteAddress, host, context.request().method(), cause);
        if (cause instanceof FileNotFoundException || cause instanceof NoSuchFileException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
        }
        else
        {
            super.processFailure(cause, context, host, remoteAddress, requestParams);
        }
    }

    private ListSnapshotFilesResponse buildResponse(String host,
                                                    String snapshotDirectory,
                                                    List<SnapshotPathBuilder.SnapshotFile> fileList)
    {
        ListSnapshotFilesResponse response = new ListSnapshotFilesResponse();
        int sidecarPort = configuration.port();
//        SnapshotDirectory directory = SnapshotDirectory.of(snapshotDirectory);
        int dataDirectoryIndex = dataDirectoryIndex(host, directory.dataDirectory);
        int offset = snapshotDirectory.length() + 1;

        for (SnapshotPathBuilder.SnapshotFile snapshotFile : fileList)
        {
            int fileNameIndex = snapshotFile.path.indexOf(snapshotDirectory) + offset;
            Preconditions.checkArgument(fileNameIndex < snapshotFile.path.length(),
                                        "Invalid snapshot file '" + snapshotFile.path + "'");
            response.addSnapshotFile(
            new ListSnapshotFilesResponse.FileInfo(snapshotFile.size,
                                                   host,
                                                   sidecarPort,
                                                   dataDirectoryIndex,
                                                   directory.snapshotName,
                                                   directory.keyspace,
                                                   maybeRemoveTableId(directory.tableName),
                                                   snapshotFile.path.substring(fileNameIndex)));
        }
        return response;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SnapshotRequest extractParamsOrThrow(final RoutingContext context)
    {
        boolean includeSecondaryIndexFiles =
        RequestUtils.parseBooleanQueryParam(context.request(), INCLUDE_SECONDARY_INDEX_FILES_QUERY_PARAM, false);

        SnapshotRequest snapshotRequest = SnapshotRequest.builder()
                                                         .qualifiedTableName(qualifiedTableName(context))
                                                         .snapshotName(context.pathParam("snapshot"))
                                                         .includeSecondaryIndexFiles(includeSecondaryIndexFiles)
                                                         .build();
        validate(snapshotRequest);
        return snapshotRequest;
    }

    private void validate(SnapshotRequest request)
    {
        validator.validateSnapshotName(request.snapshotName());
    }

    /**
     * Removes the table UUID portion from the table name if present.
     *
     * @param tableName the table name with or without the UUID
     * @return the table name without the UUID
     */
    private String maybeRemoveTableId(String tableName)
    {
        int dashIndex = tableName.lastIndexOf("-");
        if (dashIndex > 0)
        {
            return tableName.substring(0, dashIndex);
        }
        return tableName;
    }
}
