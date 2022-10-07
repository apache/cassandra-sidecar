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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.SchemaRequest;
import org.apache.cassandra.sidecar.common.data.SchemaResponse;


/**
 * The {@link SchemaHandler} class returns a
 */
@Singleton
public class SchemaHandler extends AbstractHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHandler.class);

    /**
     * Constructs a handler with the provided {@code instancesConfig}
     *
     * @param instancesConfig the instances configuration
     */
    @Inject
    protected SchemaHandler(InstancesConfig instancesConfig)
    {
        super(instancesConfig);
    }

    @Override
    public void handle(RoutingContext context)
    {
        final HttpServerRequest request = context.request();
        final String host = getHost(context);
        final SocketAddress remoteAddress = request.remoteAddress();
        final SchemaRequest requestParams = extractParamsOrThrow(context);
        final InstanceMetadata instanceMeta = instancesConfig.instanceFromHost(host);
        LOGGER.debug("SchemaHandler received request: {} from: {}. Instance: {}",
                     requestParams, remoteAddress, host);

        final Vertx vertx = context.vertx();
        getMetadata(vertx, instanceMeta).onFailure(throwable -> handleFailure(context, requestParams, throwable))
                                        .onSuccess(metadata -> handleWithMetadata(context, requestParams, metadata));
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
            LOGGER.error("Failed to obtain metadata on the connected cluster for request '{}'", requestParams);
            context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            return;
        }

        if (requestParams.getKeyspace() == null)
        {
            SchemaResponse schemaResponse = new SchemaResponse(metadata.exportSchemaAsString());
            context.json(schemaResponse);
            return;
        }

        // retrieve keyspace metadata
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(requestParams.getKeyspace());
        if (ksMetadata == null)
        {
            // set request as failed and return
            // keyspace does not exist
            String errorMessage = String.format("Keyspace '%s' does not exist.",
                                                requestParams.getKeyspace());
            context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), errorMessage));
            return;
        }

        SchemaResponse schemaResponse = new SchemaResponse(requestParams.getKeyspace(), ksMetadata.exportAsString());
        context.json(schemaResponse);
    }

    private void handleFailure(RoutingContext context, SchemaRequest requestParams, Throwable throwable)
    {
        LOGGER.error("Failed to obtain keyspace metadata for request '{}'", requestParams, throwable);
        context.fail(new HttpException(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                                       "Unable to reach the Cassandra service", throwable));
    }

    /**
     * Gets cluster metadata asynchronously.
     *
     * @param vertx            the vertx instance
     * @param instanceMetadata the instance metadata
     * @return {@link Future} containing {@link Metadata}
     */
    private Future<Metadata> getMetadata(Vertx vertx, InstanceMetadata instanceMetadata)
    {
        return vertx.executeBlocking(promise -> {
            // session() or getLocalCql() can potentially block, so move them inside a executeBlocking lambda
            Session session = instanceMetadata.session().getLocalCql();
            if (session == null)
            {
                LOGGER.error("Unable to obtain session for instance='{}' host='{}' port='{}'",
                             instanceMetadata.id(), instanceMetadata.host(), instanceMetadata.port());
                promise.fail(new RuntimeException(String.format("Could not obtain session for instance '%d'",
                                                                instanceMetadata.id())));
            }
            else
            {
                promise.complete(session.getCluster().getMetadata());
            }
        });
    }

    /**
     * Parses the request parameters
     *
     * @param rc the event to handle
     * @return the {@link SchemaRequest} parsed from the request
     */
    private SchemaRequest extractParamsOrThrow(final RoutingContext rc)
    {
        return new SchemaRequest(rc.pathParam("keyspace"));
    }
}
