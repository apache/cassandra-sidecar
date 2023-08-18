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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.MainModule.NOT_OK_STATUS;
import static org.apache.cassandra.sidecar.MainModule.OK_STATUS;

/**
 * Provides a simple REST endpoint to determine if a Cassandra node is available
 */
@Singleton
public class CassandraHealthHandler extends AbstractHandler<Void>
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected CassandraHealthHandler(InstanceMetadataFetcher metadataFetcher,
                                     ExecutorPools executorPools,
                                     CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    /**
     * Handles the request with the parameters for this request.
     *
     * @param context       the request context
     * @param httpRequest   the {@link HttpServerRequest} object
     * @param host          the host where this request is intended for
     * @param remoteAddress the address where the request originates
     * @param request       the request object
     */
    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Void request)
    {
        CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
        if (delegate != null && delegate.isUp())
        {
            context.json(OK_STATUS);
        }
        else
        {
            context.response()
                   .setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code())
                   .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                   .end(Json.CODEC.toString(NOT_OK_STATUS, false));
        }
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }
}
