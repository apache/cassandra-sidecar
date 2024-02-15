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


import java.nio.file.NoSuchFileException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.data.Name;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * This handler validates that the component exists in the cluster and sets up the context
 * for the {@link FileStreamHandler} to stream the component back to the client
 */
@Singleton
public class StreamSSTableComponentHandler extends AbstractHandler<StreamSSTableComponentRequest>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSSTableComponentHandler.class);
    private final SnapshotPathBuilder snapshotPathBuilder;

    @Inject
    public StreamSSTableComponentHandler(InstanceMetadataFetcher metadataFetcher,
                                         SnapshotPathBuilder snapshotPathBuilder,
                                         CassandraInputValidator validator,
                                         ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, validator);
        this.snapshotPathBuilder = snapshotPathBuilder;
    }

    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               StreamSSTableComponentRequest request)
    {
        Future<String> componentPathFuture;
        if (shouldUseLegacyStreamSStableComponent(request))
        {
            LOGGER.warn("Streaming SSTable component without a data directory index. " +
                        "request={}, remoteAddress={}, instance={}", request, remoteAddress, host);
            componentPathFuture = snapshotPathBuilder.build(host, request);
        }
        else
        {
            componentPathFuture = resolveComponentPathFromRequest(host, request);
        }

        componentPathFuture.onSuccess(path -> {
            logger.debug("{} resolved. path={}, request={}, remoteAddress={}, instance={}",
                         this.getClass().getSimpleName(), path, request, remoteAddress, host);
            context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path).next();
        }).onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    private Future<String> resolveComponentPathFromRequest(String host, StreamSSTableComponentRequest request)
    {
        return executorPools.internal().executeBlocking(promise -> {
            CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
            if (delegate == null)
            {
                promise.fail(cassandraServiceUnavailable());
                return;
            }

            StorageOperations storageOperations = delegate.storageOperations();
            if (storageOperations == null)
            {
                promise.fail(cassandraServiceUnavailable());
                return;
            }

            List<String> dataDirList = storageOperations.dataFileLocations();
            int dataDirIndex = request.dataDirectoryIndex();
            if (dataDirIndex < 0 || dataDirIndex >= dataDirList.size())
            {
                promise.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                               "Invalid data directory index=" + dataDirIndex));
                return;
            }

            String path = snapshotPathBuilder.resolveComponentPath(dataDirList.get(dataDirIndex), request);

            promise.complete(path);
        });
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  StreamSSTableComponentRequest request)
    {
        String errMsg = "StreamSSTableComponentHandler failed. request={}, remoteAddress={}, instance={}";
        logger.error(errMsg, request, remoteAddress, host, cause);
        if (cause instanceof NoSuchFileException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
        }
        else
        {
            super.processFailure(cause, context, host, remoteAddress, request);
        }
    }

    @Override
    protected StreamSSTableComponentRequest extractParamsOrThrow(RoutingContext context)
    {
        String tableNameParam = context.pathParam(TABLE_PATH_PARAM);
        Name tableName = validator.validateTableName(snapshotPathBuilder.maybeRemoveTableId(tableNameParam));

        QualifiedTableName qualifiedTableName = new QualifiedTableName(keyspace(context, true), tableName);
        return StreamSSTableComponentRequest.from(qualifiedTableName, context);
    }

    /**
     * @param request the request object
     * @return {@code true} if the table name does not contain the UUID, or if the index of the data directory is
     * not provided
     */
    protected boolean shouldUseLegacyStreamSStableComponent(StreamSSTableComponentRequest request)
    {
        return request.tableUuid() == null || request.dataDirectoryIndex() == null;
    }
}
