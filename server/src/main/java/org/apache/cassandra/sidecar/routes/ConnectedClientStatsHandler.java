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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.inject.Inject;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.RequestUtils.parseBooleanQueryParam;

/**
 * Handler for retrieving stats for connected clients
 */
public class ConnectedClientStatsHandler extends AbstractHandler<Boolean> implements AccessProtected
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    protected ConnectedClientStatsHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        List<String> eligibleResources = Arrays.asList(VariableAwareResource.DATA.resource(),
                                                       // Keyspace access to system_views
                                                       "data/system_views",
                                                       // Access to all tables in keyspace system_views
                                                       "data/system_views/*",
                                                       // Access to the clients table in the system_views keyspace
                                                       "data/system_views/clients");
        return Collections.singleton(CassandraPermissions.SELECT.toAuthorization(eligibleResources));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Boolean summaryOnly)
    {
        MetricsOperations operations = metadataFetcher.delegate(host).metricsOperations();
        executorPools.service()
                     .executeBlocking(() -> operations.connectedClientStats(summaryOnly))
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, summaryOnly));
    }

    protected Boolean extractParamsOrThrow(RoutingContext context)
    {
        return parseBooleanQueryParam(context.request(), "summary", true);
    }
}
