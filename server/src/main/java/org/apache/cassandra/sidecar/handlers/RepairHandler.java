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

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.job.RepairJob;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.OperationalJobUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for triggering repair
 */
public class RepairHandler extends AbstractHandler<RepairRequestParam> implements AccessProtected
{
    private final ServiceConfiguration config;
    private final OperationalJobManager jobManager;
    private final Vertx vertx;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    protected RepairHandler(Vertx vertx,
                            InstanceMetadataFetcher metadataFetcher,
                            ExecutorPools executorPools,
                            ServiceConfiguration serviceConfiguration,
                            CassandraInputValidator validator,
                            OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.vertx = vertx;
        this.jobManager = jobManager;
        this.config = serviceConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RepairRequestParam extractParamsOrThrow(RoutingContext context)
    {
        Name keyspace = keyspace(context, true);
        if (keyspace == null)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "'keyspace' is required but not supplied");
        }

        String bodyString = context.body().asString();
        if (bodyString == null || bodyString.equalsIgnoreCase("null")) // json encoder writes null as "null"
        {
            logger.warn("Bad request to create repair job. Received null payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Unexpected null payload for request");
        }

        RepairPayload payload;
        try
        {
            payload = Json.decodeValue(bodyString, RepairPayload.class);
        }
        catch (DecodeException decodeException)
        {
            logger.warn("Bad request to create repair job. Received invalid JSON payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Invalid request payload",
                                    decodeException);
        }

        return RepairRequestParam.from(keyspace, payload);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  RepairRequestParam repairRequestParam)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        RepairJob job = new RepairJob(vertx, config.repairConfiguration(), UUIDs.timeBased(), operations, repairRequestParam);
        try
        {
            jobManager.trySubmitJob(job);
        }
        catch (OperationalJobConflictException oje)
        {
            String reason = oje.getMessage();
            logger.error("Conflicting job encountered. reason={}", reason);
            context.response().setStatusCode(HttpResponseStatus.CONFLICT.code());
            context.json(new OperationalJobResponse(job.jobId(), OperationalJobStatus.FAILED, job.name(), reason));
            return;
        }

        // Get the result, waiting for the specified wait time for result
        job.asyncResult(executorPools.service(), config.operationalJobExecutionMaxWaitTime())
           .onComplete(v -> OperationalJobUtils.sendStatusBasedResponse(context, job));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.REPAIR.toAuthorization());
    }
}
