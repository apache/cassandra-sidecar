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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.request.data.NodeFlushRequestPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.job.NodeFlushJob;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides REST API for asynchronously flushing memtables for a specified keyspace and tables
 */
public class NodeFlushHandler extends AbstractHandler<NodeFlushRequestPayload> implements AccessProtected
{
    private final OperationalJobManager jobManager;
    private final ServiceConfiguration config;

    /**
     * Constructs a handler with the provided dependencies
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     * @param jobManager      the manager responsible for submitting and tracking operational jobs
     * @param config          the service configuration containing execution parameters
     */
    @Inject
    protected NodeFlushHandler(InstanceMetadataFetcher metadataFetcher,
                               ExecutorPools executorPools,
                               ServiceConfiguration config,
                               CassandraInputValidator validator,
                               OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
        this.config = config;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.FLUSH_NODE.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               NodeFlushRequestPayload payload)
    {
        String keyspace = keyspace(context, true).name();
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        NodeFlushJob job = new NodeFlushJob(UUIDs.timeBased(), operations, keyspace, payload.tableNames());
        handleOperationalJob(jobManager, config, context, job, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeFlushRequestPayload extractParamsOrThrow(RoutingContext context)
    {
        String bodyString = context.body().asString();
        if (bodyString == null || bodyString.isBlank() || bodyString.equals("null")) //json encoder writes null as "null"
        {
            return new NodeFlushRequestPayload(Collections.emptyList());
        }

        NodeFlushRequestPayload payload;

        try
        {
            payload = Json.decodeValue(bodyString, NodeFlushRequestPayload.class);
        }
        catch (DecodeException decodeException)
        {
            logger.warn("Bad request for node flush. Received invalid JSON payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Invalid request payload",
                                    decodeException);
        }

        return new NodeFlushRequestPayload(validateTableNames(payload.tableNames()));
    }

    private List<String> validateTableNames(List<String> tableNames)
    {
        return tableNames.stream()
                         .filter(tableName -> tableName != null && !tableName.isBlank())
                         .map(tableName -> validator.validateTableName(tableName).name())
                         .collect(Collectors.toList());
    }
}
