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

package org.apache.cassandra.sidecar.routes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.file.NoSuchFileException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.SnapshotRequest;
import org.apache.cassandra.sidecar.snapshots.SnapshotDirectory;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * The SnapshotsHandler class handles snapshot operations.
 *
 * <ul>
 *   <li>
 *      The <b>GET</b> verb will produce a list of paths of all the snapshot files of a given
 *      snapshot name.
 * <p>
 *      The query param {@code includeSecondaryIndexFiles} is used to request secondary index
 *      files along with other files. For example:
 * <p>
 *      {@code /api/v1/keyspaces/ks/tables/tbl/snapshots/testSnapshot}
 *      lists all SSTable component files for the <i>"testSnapshot"</i> snapshot for the
 *      <i>"ks"</i> keyspace and the <i>"tbl"</i> table
 * <p>
 *      {@code /api/v1/keyspaces/ks/tables/tbl/snapshots/testSnapshot?includeSecondaryIndexFiles=true}
 *      lists all SSTable component files, including secondary index files, for the
 *      <i>"testSnapshot"</i> snapshot for the <i>"ks"</i> keyspace and the <i>"tbl"</i> table
 *   </li>
 *   <li>
 *       The <b>PUT</b> verb creates a new snapshot for the given keyspace and table
 *   </li>
 *   <li>
 *       The <b>DELETE</b> verb deletes an existing snapshot for the given keyspace and table
 *   </li>
 * </ul>
 */
@Singleton
public class SnapshotsHandler extends AbstractHandler<SnapshotRequest>
{
    private static final String INCLUDE_SECONDARY_INDEX_FILES = "includeSecondaryIndexFiles";
    private final SnapshotPathBuilder builder;
    private final Configuration configuration;

    @Inject
    public SnapshotsHandler(SnapshotPathBuilder builder,
                            Configuration configuration,
                            InstanceMetadataFetcher metadataFetcher,
                            CassandraInputValidator validator,
                            ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, validator);
        this.builder = builder;
        this.configuration = configuration;
    }

    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               SnapshotRequest request)
    {
        HttpMethod method = context.request().method();
        if (method.equals(HttpMethod.GET))
        {
            listSnapshot(context, host, remoteAddress, request);
        }
        else if (method.equals(HttpMethod.PUT))
        {
            createSnapshot(context, host, remoteAddress, request);
        }
        else if (method.equals(HttpMethod.DELETE))
        {
            clearSnapshot(context, host, remoteAddress, request);
        }
        else
        {
            throw new UnsupportedOperationException("Method " + context.request().method() + " is not supported");
        }
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
     * @param host          the name of the host
     * @param remoteAddress the remote address that originated the request
     * @param requestParams parameters obtained from the request
     */
    private void listSnapshot(RoutingContext context,
                              String host,
                              SocketAddress remoteAddress,
                              SnapshotRequest requestParams)
    {
        builder.build(host, requestParams)
               .onSuccess(snapshotDirectory ->
                          builder.listSnapshotDirectory(snapshotDirectory, requestParams.includeSecondaryIndexFiles())
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
                                 .onFailure(cause -> processFailure(cause, context, host, remoteAddress, requestParams))
               )
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
        else if (cause instanceof HttpException)
        {
            context.fail(cause);
        }
        else
        {
            context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request for " + requestParams));
        }
    }

    private ListSnapshotFilesResponse buildResponse(String host,
                                                    String snapshotDirectory,
                                                    List<SnapshotPathBuilder.SnapshotFile> fileList)
    {
        ListSnapshotFilesResponse response = new ListSnapshotFilesResponse();
        int sidecarPort = configuration.getPort();
        SnapshotDirectory directory = SnapshotDirectory.of(snapshotDirectory);
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
                                                   directory.tableName,
                                                   snapshotFile.path.substring(fileNameIndex)));
        }
        return response;
    }

    /**
     * Creates a new snapshot for the given keyspace and table.
     *
     * @param context       the event to handle
     * @param host          the name of the host
     * @param remoteAddress the remote address that originated the request
     * @param requestParams parameters obtained from the request
     */
    private void createSnapshot(RoutingContext context,
                                String host,
                                SocketAddress remoteAddress,
                                SnapshotRequest requestParams)
    {
        ExecutorPools.TaskExecutorPool pool = executorPools.service();
        pool.executeBlocking(promise -> {
                CassandraAdapterDelegate delegate = metadataFetcher.delegate(host(context));
                StorageOperations storageOperations = delegate.storageOperations();
                if (storageOperations == null)
                    throw cassandraServiceUnavailable();
                logger.debug("Creating snapshot request={}, remoteAddress={}, instance={}",
                             requestParams, remoteAddress, host);
                storageOperations.takeSnapshot(requestParams.snapshotName(), requestParams.keyspace(),
                                               requestParams.tableName(), ImmutableMap.of());
                JsonObject jsonObject = new JsonObject()
                                        .put("result", "Success");
                context.json(jsonObject);
            })
            .onFailure(cause -> processCreateSnapshotFailure(cause, context, requestParams, remoteAddress, host));
    }

    private void processCreateSnapshotFailure(Throwable cause, RoutingContext context, SnapshotRequest requestParams,
                                              SocketAddress remoteAddress, String host)
    {
        logger.error("SnapshotsHandler failed for request={}, remoteAddress={}, instance={}, method={}",
                     requestParams, remoteAddress, host, context.request().method(), cause);

        Throwable rootCause = cause instanceof UndeclaredThrowableException
                              ? ((UndeclaredThrowableException) cause).getUndeclaredThrowable()
                              : cause;

        if (rootCause instanceof IOException)
        {
            if (StringUtils.contains(rootCause.getMessage(),
                                     "Snapshot " + requestParams.snapshotName() + " already exists"))
            {
                context.fail(wrapHttpException(HttpResponseStatus.CONFLICT, rootCause.getMessage()));
                return;
            }
            else if (StringUtils.contains(rootCause.getMessage(),
                                          "Cannot snapshot until bootstrap completes"))
            {
                // Cassandra does not allow taking snapshots while the node is JOINING the ring
                context.fail(wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                               "The Cassandra instance " + host + " is not available"));
            }
        }
        else if (rootCause instanceof IllegalArgumentException)
        {
            if (StringUtils.contains(rootCause.getMessage(),
                                     "Keyspace " + requestParams.keyspace() + " does not exist") ||
                StringUtils.contains(rootCause.getMessage(),
                                     "Unknown keyspace/cf pair"))
            {
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, rootCause.getMessage()));
            }
            else
            {
                context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, rootCause.getMessage()));
            }
            return;
        }
        context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request for " + requestParams));
    }

    /**
     * Clears a snapshot for the given keyspace and table. **Note**: Currently, Cassandra does not support
     * the table parameter. We can add support in Cassandra for the additional parameter.
     *
     * @param context       the event to handle
     * @param host          the name of the host
     * @param remoteAddress the remote address that originated the request
     * @param requestParams parameters obtained from the request
     */
    private void clearSnapshot(RoutingContext context,
                               String host,
                               SocketAddress remoteAddress,
                               SnapshotRequest requestParams)
    {
        // Leverage the SnapshotBuilder for validation purposes. Currently, JMX does not validate for
        // non-existent snapshot name or keyspace. Additionally, the current JMX implementation to clear snapshots
        // does not support passing a table as a parameter.
        builder.build(host, requestParams)
               .compose(snapshotDirectory ->
                        executorPools.service().executeBlocking(promise -> {
                            CassandraAdapterDelegate delegate =
                            metadataFetcher.delegate(host(context));
                            StorageOperations storageOperations = delegate.storageOperations();
                            if (storageOperations == null)
                                throw cassandraServiceUnavailable();
                            logger.debug("Clearing snapshot request={}, remoteAddress={}, instance={}",
                                         requestParams, remoteAddress, host);
                            storageOperations.clearSnapshot(requestParams.snapshotName(), requestParams.keyspace(),
                                                            requestParams.tableName());
                            context.response().end();
                        }))
               .onFailure(cause -> processFailure(cause, context, host, remoteAddress, requestParams));
    }

    private int dataDirectoryIndex(String host, String dataDirectory)
    {
        List<String> dataDirs = metadataFetcher.instance(host).dataDirs();
        for (int index = 0; index < dataDirs.size(); index++)
        {
            if (dataDirectory.startsWith(dataDirs.get(index)))
            {
                return index;
            }
        }
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SnapshotRequest extractParamsOrThrow(final RoutingContext context)
    {
        boolean includeSecondaryIndexFiles =
        "true".equalsIgnoreCase(context.request().getParam(INCLUDE_SECONDARY_INDEX_FILES, "false"));

        SnapshotRequest snapshotRequest = new SnapshotRequest(qualifiedTableName(context),
                                                              context.pathParam("snapshot"),
                                                              includeSecondaryIndexFiles
        );
        validate(snapshotRequest);
        return snapshotRequest;
    }

    private void validate(SnapshotRequest request)
    {
        validator.validateSnapshotName(request.snapshotName());
    }
}
