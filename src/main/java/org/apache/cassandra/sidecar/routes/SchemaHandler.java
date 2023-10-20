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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.data.SchemaResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.SchemaRequest;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * The {@link SchemaHandler} class handles schema requests
 */
@Singleton
public class SchemaHandler extends AbstractHandler<SchemaRequest>
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               SchemaRequest request)
    {
        metadata(host)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request))
        .onSuccess(metadata -> handleWithMetadata(context, request, metadata));
    }

    /**
     * Handles the request with the Cassandra {@link Metadata metadata}.
     *
     * @param context       the event to handle
     * @param requestParams the {@link SchemaRequest} parsed from the request
     * @param metadata      the metadata on the connected cluster, including known nodes and schema definitions
     */
    private void handleWithMetadata(RoutingContext context, SchemaRequest requestParams, Metadata metadata)
    {
        if (metadata == null)
        {
            // set request as failed and return
            logger.error("Failed to obtain metadata on the connected cluster for request '{}'", requestParams);
            context.fail(cassandraServiceUnavailable());
            return;
        }

        if (requestParams.keyspace() == null)
        {
            SchemaResponse schemaResponse = new SchemaResponse(metadata.exportSchemaAsString());
            context.json(schemaResponse);
            return;
        }

        // retrieve keyspace metadata
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(requestParams.keyspace());
        if (ksMetadata == null)
        {
            // set request as failed and return
            // keyspace does not exist
            String errorMessage = String.format("Keyspace '%s' does not exist.",
                                                requestParams.keyspace());
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, errorMessage));
            return;
        }

        SchemaResponse schemaResponse = new SchemaResponse(requestParams.keyspace(), ksMetadata.exportAsString());
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
        return executorPools.service().executeBlocking(promise -> {
            CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
            // metadata can block so we need to run in a blocking thread
            promise.complete(delegate.metadata());
        });
    }

    /**
     * Parses the request parameters
     *
     * @param context the event to handle
     * @return the {@link SchemaRequest} parsed from the request
     */
    @Override
    protected SchemaRequest extractParamsOrThrow(RoutingContext context)
    {
        return new SchemaRequest(keyspace(context, false));
    }
}
