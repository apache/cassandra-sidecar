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
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.server.MainModule.NOT_OK_STATUS;
import static org.apache.cassandra.sidecar.server.MainModule.OK_STATUS;


/**
 * Handler to retrieve gossip health
 */
public class GossipHealthHandler extends AbstractHandler<Void>
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    protected GossipHealthHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Void request)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        executorPools.service()
                     .executeBlocking(operations::isGossipRunning)
                     .onSuccess(isGossipRunning -> context.json(isGossipRunning ? OK_STATUS : NOT_OK_STATUS))
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }
}
