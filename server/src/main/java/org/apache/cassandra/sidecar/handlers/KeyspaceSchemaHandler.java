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
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.SchemaCache;
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
    private final SchemaCache schemaCache;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve metadata
     * @param executorPools   executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     * @param schemaCache cache of CQL schema read from Cassandra
     */
    @Inject
    protected KeyspaceSchemaHandler(InstanceMetadataFetcher metadataFetcher,
                                    ExecutorPools executorPools,
                                    CassandraInputValidator validator,
                                    SchemaCache schemaCache)
    {
        super(metadataFetcher, executorPools, validator);
        this.schemaCache = schemaCache;
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
        describeSchema(host, keyspace)
        .onSuccess(context::json)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, keyspace));
    }

    private Future<SchemaResponse> describeSchema(String host, Name keyspace)
    {
        return executorPools.service()
                            .executeBlocking(() -> {
                                if (keyspace == null)
                                {
                                    String schema = schemaCache.getFullSchema();
                                    if (schema.isEmpty())
                                    {
                                        throw schemaNotAvailable();
                                    }
                                    return new SchemaResponse(schema);
                                }

                                String keyspaceSchema = schemaCache.getKeyspaceSchema(resolveKeyspace(host, keyspace));
                                if (keyspaceSchema == null)
                                {
                                    throw keyspaceDoesNotExist(keyspace);
                                }
                                return new SchemaResponse(keyspace.name(), upperCaseReplicationClause(keyspaceSchema));
                            });
    }

    /**
     * {@code DESCRIBE KEYSPACE} emits the replication clause in lower case, whereas the schema previously returned
     * by this endpoint, generated by the Java driver, used upper case. The clause is upper cased to keep the
     * response compatible with existing clients.
     */
    private String upperCaseReplicationClause(String keyspaceSchema)
    {
        return keyspaceSchema.replaceFirst("WITH replication", "WITH REPLICATION");
    }

    /**
     * Checks keyspace exists and resolves to String stored in
     */
    private String resolveKeyspace(String host, Name keyspace)
    {
        Metadata metadata = metadataFetcher.delegate(host).metadata();
        KeyspaceMetadata ksMetadata = MetadataUtils.keyspace(metadata, keyspace);
        if (ksMetadata == null)
        {
            throw keyspaceDoesNotExist(keyspace);
        }
        return ksMetadata.getName();
    }

    private HttpException keyspaceDoesNotExist(Name keyspace)
    {
        String errorMessage = String.format("Keyspace '%s' does not exist.", keyspace.name());
        return wrapHttpException(HttpResponseStatus.NOT_FOUND, errorMessage);
    }

    private HttpException schemaNotAvailable()
    {
        return wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                 "Schema is not available yet, Cassandra could not be reached.");
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
