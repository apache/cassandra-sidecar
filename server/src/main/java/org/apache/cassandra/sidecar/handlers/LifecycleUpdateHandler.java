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

package org.apache.cassandra.sidecar.handlers;

import java.util.Collections;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.request.data.NodeCommandRequestPayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.LifecycleTaskConflictException;
import org.apache.cassandra.sidecar.lifecycle.LifecycleManager;
import org.apache.cassandra.sidecar.utils.HttpExceptions;

import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

/**
 * Handles {@code PUT /api/v1/cassandra/lifecycle} requests to start or stop a Cassandra node.
 *
 * <p> Expects a JSON payload:
 * { "state": "start" } or { "state": "stop" }
 * and will record the desired state. </p>
 */
@Singleton
public class LifecycleUpdateHandler extends NodeCommandHandler implements AccessProtected
{
    private final LifecycleManager lifecycleManager;

    @Inject
    public LifecycleUpdateHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools, LifecycleManager lifecycleManager)
    {
        super(metadataFetcher, executorPools, null);
        this.lifecycleManager = lifecycleManager;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.MODIFY_LIFECYCLE.toAuthorization());
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  NodeCommandRequestPayload request)
    {
        CassandraState desiredState = CassandraState.fromNodeCommandState(request.state());
        executorPools.service()
                     .executeBlocking(() -> lifecycleManager.updateDesiredState(host, desiredState))
                     .onSuccess(info ->
                                {
                                    HttpServerResponse response = context.response().putHeader("Content-Type", "application/json");
                                    switch (info.status())
                                    {
                                        case CONVERGED:
                                            response.setStatusCode(HttpResponseStatus.OK.code());
                                            break;
                                        case CONVERGING:
                                            response.setStatusCode(HttpResponseStatus.ACCEPTED.code());
                                            break;
                                        default:
                                            logger.warn("{} request failed with unexpected result. " +
                                                        "request={}, remoteAddress={}, instance={}, operationStatus={}",
                                                        this.getClass().getSimpleName(), request, remoteAddress, host, info.status());
                                            response.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                                    }
                                    response.end(Json.encode(info));
                                })
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected void processFailure(Throwable cause, RoutingContext context, String host, SocketAddress remoteAddress, NodeCommandRequestPayload request)
    {
        if (cause instanceof LifecycleTaskConflictException)
        {
            HttpException httpException = HttpExceptions.wrapHttpException(HttpResponseStatus.CONFLICT,
                                                                           cause.getMessage(), cause);
            logger.warn("{} request failed due to lifecycle conflict. request={}, remoteAddress={}, instance={}",
                        this.getClass().getSimpleName(), request, remoteAddress, host, cause);
            context.fail(httpException);
        }
        else
        {
            super.processFailure(cause, context, host, remoteAddress, request);
        }
    }
}
