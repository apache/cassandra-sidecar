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
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.request.data.NodeCommandRequestPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.modules.ApiModule.OK_STATUS;


/**
 * Handler for starting or stopping native transport on a Cassandra node.
 * <p>
 * Expects a JSON body:
 * { "state": "start" }  or  { "state": "stop" }
 * </p>
 */
@Singleton
public class NativeUpdateHandler extends NodeCommandHandler implements AccessProtected
{
    @Inject
    public NativeUpdateHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.MODIFY_NATIVE.toAuthorization());
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  NodeCommandRequestPayload request)
    {
        StorageOperations storageOps = metadataFetcher.delegate(host).storageOperations();

        executorPools.service().runBlocking(() -> {
            switch (request.state())
            {
                case START:
                    storageOps.startNativeTransport();
                    break;
                case STOP:
                    storageOps.stopNativeTransport();
                    break;
                default:
                    throw new IllegalStateException("Unknown state: " + request.state());
            }
        })
                     .onSuccess(ignored -> context.json(OK_STATUS))
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }
}

