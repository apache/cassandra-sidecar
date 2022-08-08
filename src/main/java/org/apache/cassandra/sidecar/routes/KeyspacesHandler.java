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

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.data.KeyspaceRequest;
import org.apache.cassandra.sidecar.common.data.KeyspaceSchema;
import org.apache.cassandra.sidecar.common.data.TableSchema;
import org.jetbrains.annotations.NotNull;

/**
 * Handler for getting information about keyspace / tables from a specific Cassandra instance
 */
@Singleton
public class KeyspacesHandler extends AbstractHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyspacesHandler.class);
    private final Vertx vertx;

    @Inject
    public KeyspacesHandler(Vertx vertx, InstancesConfig instancesConfig)
    {
        super(instancesConfig);
        this.vertx = vertx;
    }

    /**
     * Handles {@code GET} and {@code HEAD} requests for Keyspaces.
     * For keyspaces requests only {@code GET} is supported, and it will produce a list of keyspaces with table and
     * schema information. For keyspace and table requests both {@code GET} and {@code HEAD} requests are supported.
     *
     * @param context the event to handle
     */
    @Override
    public void handle(RoutingContext context)
    {
        KeyspaceRequest requestParams = extractParamsOrThrow(context);
        String host = getHost(context);
        SocketAddress remoteAddress = context.request().remoteAddress();
        InstanceMetadata instanceMeta = instancesConfig.instanceFromHost(host);
        LOGGER.debug("KeyspacesHandler received request: {} from: {}. Instance: {}",
                     requestParams, remoteAddress, host);

        getMetadata(instanceMeta.session())
        .onFailure(throwable -> {
            LOGGER.error("Failed to obtain keyspace metadata for request '{}'", requestParams, throwable);
            context.fail(new HttpException(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                                           "Unable to reach Cassandra service",
                                           throwable));
        })
        .onSuccess(metadata ->
                   {
                       KeyspaceMetadata ksMetadata;
                       if (metadata == null)
                       {
                           LOGGER.error("Failed to obtain keyspace metadata for request '{}'", requestParams);
                           context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                       }
                       else if (requestParams.getKeyspace() == null)
                       {
                           // keyspaces request
                           buildKeyspacesResponse(context, metadata);
                       }
                       else if ((ksMetadata = metadata.getKeyspace(requestParams.getKeyspace())) == null)
                       {
                           // keyspace does not exist
                           String errorMessage = String.format("Keyspace '%s' does not exist.",
                                                               requestParams.getKeyspace());
                           context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), errorMessage));
                       }
                       else if (requestParams.getTableName() == null)
                       {
                           // keyspace request
                           buildKeyspaceResponse(context, ksMetadata);
                       }
                       else
                       {
                           TableMetadata tableMetadata = ksMetadata.getTable(requestParams.getTableName());

                           if (tableMetadata == null)
                           {
                               String errorMessage = String.format("Table '%s' does not exist in the '%s' keyspace",
                                                                   requestParams.getTableName(),
                                                                   requestParams.getKeyspace());
                               context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), errorMessage));
                           }
                           else
                           {
                               // keyspace / table request
                               buildTableResponse(context, tableMetadata);
                           }
                       }
                   });
    }

    /**
     * Builds the response for the keyspaces request, listing all keyspaces available in the cluster.
     *
     * @param context  the context to handle
     * @param metadata the metadata on the connected cluster, including known nodes and schema definitions
     */
    private void buildKeyspacesResponse(RoutingContext context, @NotNull Metadata metadata)
    {
        if (context.request().method() == HttpMethod.HEAD)
        {
            // head is not supported for keyspaces
            context.fail(HttpResponseStatus.BAD_REQUEST.code());
        }
        else
        {
            context.json(metadata.getKeyspaces().stream().map(KeyspaceSchema::of).collect(Collectors.toList()));
        }
    }

    /**
     * Builds the response for the provided {@code keyspaceMetadata}. For {@link HttpMethod#HEAD} requests,
     * end the request successfully.
     *
     * @param context          the context to handle
     * @param keyspaceMetadata the object that describes a keyspace defined in the Cassandra cluster
     */
    private void buildKeyspaceResponse(RoutingContext context, @NotNull KeyspaceMetadata keyspaceMetadata)
    {
        if (context.request().method() == HttpMethod.HEAD)
        {
            context.response().end();
        }
        else
        {
            context.json(KeyspaceSchema.of(keyspaceMetadata));
        }
    }

    /**
     * Builds the response for the provided {@code tableMetadata}. If {@code tableMetadata} is {@code null}, respond
     * with a {@link HttpResponseStatus#NOT_FOUND} response code. For {@link HttpMethod#HEAD} requests, end the request
     * successfully.
     *
     * @param context       the context to handle
     * @param tableMetadata the object that describes a table defined in the Cassandra cluster
     */
    private void buildTableResponse(RoutingContext context, @NotNull TableMetadata tableMetadata)
    {
        if (context.request().method() == HttpMethod.HEAD)
        {
            context.response().end();
        }
        else
        {
            context.json(TableSchema.of(tableMetadata));
        }
    }

    /**
     * Gets cluster metadata asynchronously.
     *
     * @param cqlSession session object for a particular Cassandra instance
     * @return {@link Future} containing {@link Metadata}
     */
    private Future<Metadata> getMetadata(CQLSession cqlSession)
    {
        return vertx.executeBlocking(promise -> {
            Session session = cqlSession.getLocalCql();
            if (session == null)
            {
                promise.fail(new RuntimeException("Could not obtain session object"));
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
     * @return the {@link KeyspaceRequest} parsed from the request
     */
    private KeyspaceRequest extractParamsOrThrow(final RoutingContext rc)
    {
        return new KeyspaceRequest(rc.pathParam("keyspace"),
                                   rc.pathParam("table"));
    }
}
