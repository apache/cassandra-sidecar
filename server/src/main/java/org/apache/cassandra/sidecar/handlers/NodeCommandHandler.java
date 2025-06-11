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

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.request.data.NodeCommandRequestPayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Base handler for "node command" endpoints (e.g. start/stop gossip or native transport).
 * Extracts a {@link NodeCommandRequestPayload} from the request body, validates it,
 * and defers to subclasses in {@link #handleInternal} to perform the actual operation.
 */
public abstract class NodeCommandHandler extends AbstractHandler<NodeCommandRequestPayload>
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    protected NodeCommandHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools, CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    /**
     * extract node command from RoutingContext
     *
     * @param ctx the request context
     * @return parsed NodeCommandRequestPayload
     */
    @Override
    protected NodeCommandRequestPayload extractParamsOrThrow(RoutingContext ctx)
    {
        String body = ctx.body().asString();
        if (body == null || body.equalsIgnoreCase("null"))
        {
            logger.warn("Bad request. Received null payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Request body must be JSON with a non-null \"state\" field");
        }
        try
        {
            return Json.decodeValue(body, NodeCommandRequestPayload.class);
        }
        catch (DecodeException e)
        {
            logger.warn("Bad request. Received invalid JSON payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid state value: " + e.getMessage());
        }
    }
}
