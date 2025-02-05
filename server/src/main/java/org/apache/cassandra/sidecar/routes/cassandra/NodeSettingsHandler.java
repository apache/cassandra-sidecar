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

package org.apache.cassandra.sidecar.routes.cassandra;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

/**
 * Provides REST endpoint to get the configured settings of a cassandra node.
 * <p>
 * Note: {@link NodeSettingsHandler} is not Access protected. Any user who can log in into Cassandra is able to view
 * node settings information. Since sidecar and cassandra share list of authenticated identities, sidecar's
 * authenticated users can also read node settings information.
 */
@Singleton
public class NodeSettingsHandler extends AbstractHandler<Void>
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     */
    @Inject
    NodeSettingsHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools)
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
        context.json(metadataFetcher.delegate(host).nodeSettings());
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
