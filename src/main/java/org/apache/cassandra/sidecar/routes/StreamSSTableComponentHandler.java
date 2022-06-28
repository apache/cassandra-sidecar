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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;

/**
 * This handler validates that the component exists in the cluster and sets up the context
 * for the {@link FileStreamHandler} to stream the component back to the client
 */
@Singleton
public class StreamSSTableComponentHandler extends AbstractHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSSTableComponentHandler.class);

    private final SnapshotPathBuilder snapshotPathBuilder;

    @Inject
    public StreamSSTableComponentHandler(SnapshotPathBuilder snapshotPathBuilder, InstancesConfig instancesConfig)
    {
        super(instancesConfig);
        this.snapshotPathBuilder = snapshotPathBuilder;
    }

    @Override
    public void handle(RoutingContext context)
    {
        final HttpServerRequest request = context.request();
        final String host = getHost(context);
        final SocketAddress remoteAddress = request.remoteAddress();
        final StreamSSTableComponentRequest requestParams = extractParamsOrThrow(context);
        logger.debug("StreamSSTableComponentHandler received request: {} from: {}. Instance: {}", requestParams,
                     remoteAddress, host);

        snapshotPathBuilder.build(host, requestParams)
                           .onSuccess(path ->
                                      {
                                          logger.debug("StreamSSTableComponentHandler handled {} for client {}. Instance: {}", path,
                                                       remoteAddress, host);
                                          context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path)
                                                 .next();
                                      })
                           .onFailure(cause ->
                                      {
                                          if (cause instanceof FileNotFoundException)
                                          {
                                              context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), cause.getMessage()));
                                          }
                                          else
                                          {
                                              context.fail(new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                                                             "Invalid request for " + requestParams));
                                          }
                                      });
    }

    private StreamSSTableComponentRequest extractParamsOrThrow(final RoutingContext rc)
    {
        return new StreamSSTableComponentRequest(rc.pathParam("keyspace"),
                                                 rc.pathParam("table"),
                                                 rc.pathParam("snapshot"),
                                                 rc.pathParam("component")
        );
    }
}
