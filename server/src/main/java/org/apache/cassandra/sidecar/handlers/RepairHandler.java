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

import java.math.BigInteger;
import java.util.Collections;
import java.util.Set;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.job.RepairJob;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
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
    private final PeriodicTaskExecutor periodicTaskExecutor;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher      the metadata fetcher
     * @param executorPools        executor pools for blocking executions
     * @param serviceConfiguration configuration object holding config details of Sidecar
     * @param validator            a validator instance to validate Cassandra-specific input
     * @param jobManager           manager for long-running operational jobs
     * @param periodicTaskExecutor executor for periodic tasks
     */
    @Inject
    protected RepairHandler(InstanceMetadataFetcher metadataFetcher,
                            ExecutorPools executorPools,
                            ServiceConfiguration serviceConfiguration,
                            CassandraInputValidator validator,
                            OperationalJobManager jobManager,
                            PeriodicTaskExecutor periodicTaskExecutor)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
        this.config = serviceConfiguration;
        this.periodicTaskExecutor = periodicTaskExecutor;
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

        RepairPayload payload;
        try
        {
            payload = context.body().asPojo(RepairPayload.class);
        }
        catch (DecodeException decodeException)
        {
            logger.warn("Bad request to create repair job. Received invalid JSON payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Invalid request payload",
                                    decodeException);
        }
        if (payload == null)
        {
            logger.warn("Bad request to create repair job. Received null payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Unexpected null payload for request");
        }

        // Validate token range if provided
        validateTokenRange(payload);

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
        RepairJob job = new RepairJob(periodicTaskExecutor, config.repairConfiguration(), UUIDs.timeBased(), operations, repairRequestParam);

        jobManager.trySubmitJob(job,
                                (completedJob, exception) ->
                                OperationalJobUtils.sendStatusBasedResponse(context, completedJob, exception),
                                executorPools.service(),
                                config.operationalJobExecutionMaxWaitTime());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.REPAIR.toAuthorization());
    }

    /**
     * Validates the token range in the repair payload if both start and end tokens are provided.
     *
     * @param payload the repair payload to validate
     * @throws HttpException if the token range is invalid
     */
    private void validateTokenRange(RepairPayload payload)
    {
        if (payload.startToken() != null && payload.endToken() != null)
        {
            try
            {
                String startTokenStr = payload.startToken();
                String endTokenStr = payload.endToken();

                // Validate tokens using BigInteger for proper numeric comparison
                BigInteger startToken = new BigInteger(startTokenStr);
                BigInteger endToken = new BigInteger(endTokenStr);

                if (startToken.compareTo(endToken) >= 0)
                {
                    throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                            "Start token must be less than end token. Got start: " +
                                            startTokenStr + ", end: " + endTokenStr);
                }
            }
            catch (NumberFormatException e)
            {
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                        "Invalid token format. Tokens must be numeric values.", e);
            }
        }
    }
}
