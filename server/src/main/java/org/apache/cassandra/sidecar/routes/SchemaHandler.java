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

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.SidecarPermissions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * The {@link SchemaHandler} class handles schema requests
 */
@Singleton
public class SchemaHandler extends AbstractHandler<Void> implements AccessProtected
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve metadata
     * @param executorPools   executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected SchemaHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools,
                            CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        String resource = VariableAwareResource.CLUSTER.resource();
        return ImmutableSet.of(SidecarPermissions.VIEW_SCHEMA.toAuthorization(resource));
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
        metadata(host)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request))
        .onSuccess(metadata -> handleWithMetadata(context, metadata));
    }

    /**
     * Handles the request with the Cassandra {@link Metadata metadata}.
     *
     * @param context  the event to handle
     * @param metadata the metadata on the connected cluster, including known nodes and schema definitions
     */
    private void handleWithMetadata(RoutingContext context, Metadata metadata)
    {
        SchemaResponse schemaResponse = new SchemaResponse(metadata.exportSchemaAsString());
        context.json(schemaResponse);
    }

    /**
     * Gets cluster metadata asynchronously.
     *
     * @param host the Cassandra instance host
     * @return {@link Future} containing {@link Metadata}
     */
    private Future<Metadata> metadata(String host)
    {
        return executorPools.service().executeBlocking(() -> {
            // metadata can block so we need to run in a blocking thread
            return metadataFetcher.delegate(host).metadata();
        });
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
