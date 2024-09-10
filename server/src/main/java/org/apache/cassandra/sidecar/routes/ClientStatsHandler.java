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

import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.RequestUtils.parseBooleanQueryParam;

/**
 * Handler for retrieving client stats
 */
public class ClientStatsHandler extends AbstractHandler<Void>
{

    public enum ClientStatsParams
    {
        listConnections("list-connections"),
        byProtocol("by-protocol"),
        verbose("verbose"),
        clientOptions("client-options");

        private final String value;

        ClientStatsParams(String value)
        {
            this.value = value;
        }

        String getValue()
        {
            return value;
        }
    }
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    protected ClientStatsHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               Void request)
    {

        CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
        if (delegate == null)
        {
            context.fail(cassandraServiceUnavailable());
            return;
        }
        MetricsOperations operations = delegate.metricsOperations();
        Metadata metadata = delegate.metadata();
        if (operations == null || metadata == null)
        {
            context.fail(cassandraServiceUnavailable());
            return;
        }

        boolean isListConnections = parseBooleanQueryParam(httpRequest,"list-connections", false);
        boolean isVerbose = parseBooleanQueryParam(httpRequest,"verbose", false);
        boolean isByProtocol = parseBooleanQueryParam(httpRequest,"by-protocol", false);
        boolean isClientOptions = parseBooleanQueryParam(httpRequest,"client-options", false);

        executorPools.service()
                     .executeBlocking(() -> operations.clientStats(isListConnections,
                                                                   isVerbose,
                                                                   isByProtocol,
                                                                   isClientOptions))
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }
}
