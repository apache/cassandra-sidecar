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
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.request.data.NodeMoveRequestPayload;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.utils.StringUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.job.NodeMoveJob;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.OperationalJobUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides REST API for asynchronously moving the corresponding Cassandra node to a new token
 */
@Singleton
public class NodeMoveHandler extends AbstractHandler<String> implements AccessProtected
{
    /**
     * Maximum allowed length for token strings in characters.
     * <p>
     * This limit is set conservatively to accommodate all valid token formats, including
     * the string representation of {@code Long.MIN_VALUE} (-9223372036854775808, 20 characters)
     * with substantial margin for other token representations or future extensions.
     */
    private static final int MAX_TOKEN_LENGTH = 128;

    private final OperationalJobManager jobManager;
    private final ServiceConfiguration config;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected NodeMoveHandler(InstanceMetadataFetcher metadataFetcher,
                              ExecutorPools executorPools,
                              ServiceConfiguration serviceConfiguration,
                              CassandraInputValidator validator,
                              OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
        this.config = serviceConfiguration;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.MOVE_NODE.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               String newToken)
    {
        CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
        StorageOperations operations = delegate.storageOperations();
        NodeSettings nodeSettings = delegate.nodeSettings();
        UUID nodeId = nodeSettings.hostId();
        NodeMoveJob job = new NodeMoveJob(UUIDs.timeBased(), nodeId, newToken, operations);
        this.jobManager.trySubmitJob(job,
                                     (completedJob, exception) ->
                                     OperationalJobUtils.sendStatusBasedResponse(context, completedJob, exception),
                                     executorPools.service(),
                                     config.operationalJobExecutionMaxWaitTime());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        String body = context.body().asString();
        if (body == null || body.equalsIgnoreCase("null"))
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Request body must be JSON with a non-null \"newToken\" field");
        }
        try
        {
            NodeMoveRequestPayload payload = Json.decodeValue(body, NodeMoveRequestPayload.class);
            String newToken = payload.newToken();
            if (StringUtils.isNullOrEmpty(newToken))
            {
                throw new IllegalArgumentException("newToken value cannot be null or empty");
            }

            String trimmedToken = newToken.trim();
            if (trimmedToken.length() >= MAX_TOKEN_LENGTH)
            {
                throw new IllegalArgumentException(
                String.format("newToken value must be less than %d characters. Provided value length=%d",
                              MAX_TOKEN_LENGTH, trimmedToken.length()));
            }
            return trimmedToken;
        }
        catch (DecodeException e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Failed to parse NodeMoveRequestPayload error=" + e.getMessage());
        }
    }
}
