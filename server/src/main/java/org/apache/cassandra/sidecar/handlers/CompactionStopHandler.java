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

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.data.CompactionStopStatus;
import org.apache.cassandra.sidecar.common.data.CompactionType;
import org.apache.cassandra.sidecar.common.request.data.CompactionStopRequestPayload;
import org.apache.cassandra.sidecar.common.response.CompactionStopResponse;
import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for stopping compaction operations via the Cassandra Compaction Manager.
 *
 * <p>Handles {@code POST /api/v1/cassandra/operations/compaction/stop} requests to stop
 * compaction operations. Expects a JSON payload with compaction_type and/or compaction_id:
 * <pre>
 *   { "compaction_type": "COMPACTION", "compaction_id": "abc-123" }
 * </pre>
 */
public class CompactionStopHandler extends AbstractHandler<CompactionStopRequestPayload> implements AccessProtected
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   executor pools for blocking executions
     * @param validator       validator for Cassandra-specific input
     */
    @Inject
    protected CompactionStopHandler(final InstanceMetadataFetcher metadataFetcher,
                                    final ExecutorPools executorPools,
                                    final CassandraInputValidator validator) {
        super(metadataFetcher, executorPools, validator);
    }

    @Override
    public Set<Authorization> requiredAuthorizations() {
        return Collections.singleton(BasicPermissions.MODIFY_COMPACTION.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               CompactionStopRequestPayload request) {
        CompactionManagerOperations compactionManagerOps = metadataFetcher.delegate(host).compactionManagerOperations();

        executorPools.service()
                .executeBlocking(() -> stopCompaction(compactionManagerOps, request))
                .onSuccess(context::json)
                .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    /**
     * Stops the compaction based on the request parameters
     *
     * @param operations the compaction manager operations
     * @param request    the request payload containing compaction_type and/or compaction_id
     * @return CompactionStopResponse with the operation result
     */
    private CompactionStopResponse stopCompaction(CompactionManagerOperations operations,
                                                  CompactionStopRequestPayload request)
    {
        CompactionType compactionType = request.compactionType();
        String compactionId = request.compactionId();

        // Convert enum to string for the operation (if not null)
        // Use .name() to get uppercase, which Cassandra's OperationType expects
        String compactionTypeStr = compactionType != null ? compactionType.name() : null;

        // Attempt to stop the compaction
        // If compactionId is provided, use it (takes precedence over type)
        if (compactionId != null && !compactionId.trim().isEmpty()) {
            operations.stopCompactionById(compactionId);
        } else if (compactionType != null) {
            operations.stopCompaction(compactionTypeStr);
        }
        // If we reach here, at least one of the above conditions was true due to validation in extractParamsOrThrow()
        // Return success response
        return CompactionStopResponse.builder()
                .compactionType(compactionType)
                .compactionId(compactionId)
                .status(CompactionStopStatus.SUBMITTED)
                .build();
    }

    /**
     * Override extractParamsOrThrow to support compactionStop param constraints
     * Method extracts and validates compaction stop request from routing context
     *
     * @param context the routing context
     * @return validated CompactionStopRequestPayload
     */
    @Override
    protected CompactionStopRequestPayload extractParamsOrThrow(RoutingContext context)
    {
        String body = context.body().asString();
        CompactionStopRequestPayload payload;

        /*
         Return 400 BAD_REQUEST upon malformed JSON - avoids falling under processFailure()'s vague
         500 INTERNAL_SERVER_ERROR. Also catches invalid compaction types via CompactionType.fromString()
        */
        try
        {
            payload = Json.decodeValue(body, CompactionStopRequestPayload.class);
        }
        catch (DecodeException e)
        {
            logger.warn("Bad request. Received invalid JSON payload: {}", e.getMessage());
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                    "Invalid JSON payload: " + e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            logger.warn("Bad request. {}", e.getMessage());
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }

        // Validate that at least one field is provided
        if (payload.compactionType() == null &&
            (payload.compactionId() == null || payload.compactionId().trim().isEmpty()))
        {
            logger.warn("Bad request. Both compaction_type and compaction_id are missing.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                    "At least one of 'compaction_type' or 'compaction_id' must be provided");
        }

        return payload;
    }
}