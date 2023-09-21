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
import java.util.Objects;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * This handler validates that the component exists in the cluster and sets up the context
 * for the {@link FileStreamHandler} to stream the component back to the client
 */
@Singleton
public class StreamSSTableComponentHandler extends AbstractHandler<StreamSSTableComponentRequest>
{
    private final SnapshotPathBuilder snapshotPathBuilder;
    private final CassandraInputValidator validator;

    @Inject
    public StreamSSTableComponentHandler(InstanceMetadataFetcher metadataFetcher,
                                         SnapshotPathBuilder snapshotPathBuilder,
                                         CassandraInputValidator validator,
                                         ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, validator);
        this.snapshotPathBuilder = snapshotPathBuilder;
        this.validator = validator;
    }

    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               StreamSSTableComponentRequest request)
    {
        snapshotPathBuilder.build(host, request)
                           .onSuccess(path -> {
                               logger.debug("StreamSSTableComponentHandler handled {} for client {}. "
                                            + "Instance: {}", path, remoteAddress, host);
                               context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path)
                                      .next();
                           })
                           .onFailure(cause -> {
                               String errMsg =
                               "StreamSSTableComponentHandler failed for request: {} from: {}. Instance: {}";
                               logger.error(errMsg, request, remoteAddress, host, cause);
                               if (cause instanceof NoSuchFileException)
                               {
                                   context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
                               }
                               else
                               {
                                   context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                                                  "Invalid request for " + request));
                               }
                           });
    }

    @Override
    protected StreamSSTableComponentRequest extractParamsOrThrow(RoutingContext context)
    {
        return StreamSSTableComponentRequest.from(qualifiedTableName(context), context);
    }
}
