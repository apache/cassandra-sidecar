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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.DriverUnsupportedSchemaCache;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.MetadataUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * The {@link KeyspaceSchemaHandler} class handles keyspace schema requests
 */
@Singleton
public class KeyspaceSchemaHandler extends AbstractHandler<Name> implements AccessProtected
{
    private final DriverUnsupportedSchemaCache driverUnsupportedSchemaCache;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve metadata
     * @param executorPools   executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     * @param driverUnsupportedSchemaCache cache of unparseable table schemas by Java driver
     */
    @Inject
    protected KeyspaceSchemaHandler(InstanceMetadataFetcher metadataFetcher,
                                    ExecutorPools executorPools,
                                    CassandraInputValidator validator,
                                    DriverUnsupportedSchemaCache driverUnsupportedSchemaCache)
    {
        super(metadataFetcher, executorPools, validator);
        this.driverUnsupportedSchemaCache = driverUnsupportedSchemaCache;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.READ_SCHEMA_KEYSPACE_SCOPED.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Name keyspace)
    {
        Future.all(metadata(host), unsupportedSchema(keyspace))
              .onFailure(cause -> processFailure(cause, context, host, remoteAddress, keyspace))
              .onSuccess(future -> handleWithMetadata(context, keyspace, future));
    }

    /**
     * Handles the request with the Cassandra {@link Metadata metadata}.
     *
     * @param context  the event to handle
     * @param keyspace the keyspace parsed from the request
     * @param future   composite future containing result of:
     *                 - the metadata on the connected cluster, including known nodes and schema definitions
     *                 - additional schema which could not be parsed by Java driver
     */
    private void handleWithMetadata(RoutingContext context, Name keyspace, CompositeFuture future)
    {
        Metadata metadata = future.resultAt(0);
        String unparseableSchema = future.resultAt(1);
        if (keyspace == null)
        {
            String fullSchema = DriverUnsupportedSchemaCache.concatSchemas(metadata.exportSchemaAsString(),
                                                                           unparseableSchema);
            SchemaResponse schemaResponse = new SchemaResponse(fullSchema);
            context.json(schemaResponse);
            return;
        }

        // retrieve keyspace metadata
        KeyspaceMetadata ksMetadata = MetadataUtils.keyspace(metadata, keyspace);

        if (ksMetadata == null)
        {
            // set request as failed and return
            // keyspace does not exist
            String errorMessage = String.format("Keyspace '%s' does not exist.", keyspace);
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, errorMessage));
            return;
        }

        String keyspaceSchema = DriverUnsupportedSchemaCache.concatSchemas(ksMetadata.exportAsString(),
                                                                           unparseableSchema);
        SchemaResponse schemaResponse = new SchemaResponse(keyspace.name(), keyspaceSchema);
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
     * Get CQL schema not parseable by Java driver (and therefore not present in {@link Metadata}).
     *
     * @param keyspace optional keyspace name, {@code null} includes schema for all keyspaces
     * @return {@link Future} containing CQL schema
     */
    private Future<String> unsupportedSchema(Name keyspace)
    {
        return executorPools.service().executeBlocking(() -> {
            // schema cache can block if it was not successfully initialized at least once
            if (keyspace == null)
            {
                return driverUnsupportedSchemaCache.getFullSchema();
            }
            else
            {
                return driverUnsupportedSchemaCache.getKeyspaceSchema(keyspace);
            }
        });
    }

    /**
     * Parses the request parameters
     *
     * @param context the event to handle
     * @return the keyspace parsed from the request
     */
    @Override
    protected Name extractParamsOrThrow(RoutingContext context)
    {
        return keyspace(context, true);
    }
}
