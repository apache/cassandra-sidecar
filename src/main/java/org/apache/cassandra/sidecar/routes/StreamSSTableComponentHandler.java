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
import javax.management.InstanceNotFoundException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.TableOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;
import org.apache.cassandra.sidecar.routes.data.StreamSSTableComponentRequestParam;
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
public class StreamSSTableComponentHandler extends AbstractHandler<StreamSSTableComponentRequestParam>
{
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
                               StreamSSTableComponentRequestParam request)
    {
        resolveComponentPathFromRequest(host, request).onSuccess(path -> {
            logger.debug("{} resolved. path={}, request={}, remoteAddress={}, instance={}",
                         this.getClass().getSimpleName(), path, request, remoteAddress, host);
            context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path).next();
        }).onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    private Future<String> resolveComponentPathFromRequest(String host, StreamSSTableComponentRequestParam request)
    {
        return executorPools.internal().executeBlocking(() -> {
            CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
            if (delegate == null)
            {
                throw cassandraServiceUnavailable();
            }

            int dataDirIndex = request.dataDirectoryIndex();
            if (request.tableId() != null)
            {
                StorageOperations storageOperations = delegate.storageOperations();
                if (storageOperations == null)
                {
                    throw cassandraServiceUnavailable();
                }

                List<String> dataDirList = storageOperations.dataFileLocations();
                if (dataDirIndex < 0 || dataDirIndex >= dataDirList.size())
                {
                    throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid data directory index: " + dataDirIndex);
                }
                return snapshotPathBuilder.resolveComponentPathFromDataDirectory(dataDirList.get(dataDirIndex), request);
            }
            else
            {
                logger.debug("Streaming SSTable component without a table Id. request={}, instance={}", request, host);
                TableOperations tableOperations = delegate.tableOperations();
                if (tableOperations == null)
                {
                    throw cassandraServiceUnavailable();
                }

                // asking jmx to give us the path for keyspace/table - tableId
                // as opposed to storageOperations.dataFileLocations, the table directory can change
                // when someone drops a table and recreates it with the same name, the table id will change
                // we do not keep a cache of the table directory data paths, so these requests always go
                // through JMX
                List<String> tableDirList = tableOperations.getDataPaths(request.keyspace(), request.tableName());
                if (dataDirIndex < 0 || dataDirIndex >= tableDirList.size())
                {
                    throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid data directory index: " + dataDirIndex);
                }

                return snapshotPathBuilder.resolveComponentPathFromTableDirectory(tableDirList.get(dataDirIndex), request);
            }
        });
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  StreamSSTableComponentRequestParam request)
    {
        String errMsg = "StreamSSTableComponentHandler failed. request={}, remoteAddress={}, instance={}";
        logger.error(errMsg, request, remoteAddress, host, cause);
        if (cause instanceof NoSuchFileException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
        }
        else
        {
            InstanceNotFoundException instanceNotFoundException = ThrowableUtils.getCause(cause, InstanceNotFoundException.class);
            if (instanceNotFoundException != null)
            {
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, "keyspace/table combination not found"));
            }
            else
            {
                super.processFailure(cause, context, host, remoteAddress, request);
            }
        }
    }

    @Override
    protected StreamSSTableComponentRequestParam extractParamsOrThrow(RoutingContext context)
    {
        String tableNameParam = context.pathParam(TABLE_PATH_PARAM);
        Name tableName = validator.validateTableName(snapshotPathBuilder.maybeRemoveTableId(tableNameParam));

        QualifiedTableName qualifiedTableName = new QualifiedTableName(keyspace(context, true), tableName);
        return StreamSSTableComponentRequestParam.from(qualifiedTableName, context);
    }
}
