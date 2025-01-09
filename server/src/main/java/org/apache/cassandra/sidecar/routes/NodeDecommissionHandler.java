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

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeDecommissionResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.apache.cassandra.sidecar.job.DecommissionJob;
import org.apache.cassandra.sidecar.job.OperationalJob;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;
import static org.apache.cassandra.sidecar.utils.RequestUtils.parseBooleanQueryParam;

/**
 * Provides REST API for asynchronously decommissioning the corresponding Cassandra node
 */
public class NodeDecommissionHandler extends AbstractHandler<Void>
{
    private final OperationalJobManager jobManager;
    private final ServiceConfiguration config;
    private boolean isForce;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected NodeDecommissionHandler(InstanceMetadataFetcher metadataFetcher,
                                      ExecutorPools executorPools,
                                      ServiceConfiguration serviceConfiguration,
                                      CassandraInputValidator validator,
                                      OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
        this.config = serviceConfiguration;
    }

    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    protected void handleInternal(RoutingContext context, HttpServerRequest httpRequest, String host, SocketAddress remoteAddress, Void request)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        isForce = parseBooleanQueryParam(context.request(), "force", false);

        OperationalJob job = new DecommissionJob(UUIDs.timeBased(), operations, isForce);
        jobManager.trySubmitJob(job);

        job.asyncResult(executorPools.service(),
                        Duration.of(config.operationalJobExecutionMaxWaitTimeInMillis(), ChronoUnit.MILLIS))
           .onSuccess(f -> sendResponse(context, job, request, remoteAddress, host))
           .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    private void sendResponse(RoutingContext context, OperationalJob job, Void request, SocketAddress remoteAddress, String host)
    {
        OperationalJobStatus jobStatus = job.status();
        logger.info("Job completion status={} request={} remoteAddress={} instance={}",
                    jobStatus, request, remoteAddress, host);

        String reason = null;
        switch(jobStatus)
        {
            case SUCCEEDED:
                context.response().setStatusCode(HttpResponseStatus.OK.code());
                break;
            case FAILED:
                reason = job.asyncResult().cause().getMessage();
                context.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                break;
            case CREATED:
            case RUNNING:
                context.response().setStatusCode(HttpResponseStatus.ACCEPTED.code());
                break;
            default:
                throw new IllegalArgumentException("Unexpected job status encountered: " + jobStatus);
        }
        context.json(new NodeDecommissionResponse(job.jobId, jobStatus, host, reason));
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Void request)
    {
        if (cause instanceof OperationalJobConflictException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.CONFLICT, cause.getMessage(), cause));
            return;
        }
        super.processFailure(cause, context, host, remoteAddress, request);
    }
}
