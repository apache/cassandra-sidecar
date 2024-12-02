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

import java.util.UUID;
import javax.inject.Inject;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.response.OperationsJobsResponse;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.job.OperationsJob;
import org.apache.cassandra.sidecar.job.OperationsJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.OPERATIONS_JOB_ID_PATH_PARAM;
import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.OPERATIONS_JOB_HEADER_NAME;
import static org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus.COMPLETED;
import static org.apache.cassandra.sidecar.common.utils.OperationsJobResult.OperationsJobStatus.FAILED;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for retrieving the status of async operations jobs running on the sidecar
 */
public class OperationsJobsHandler extends AbstractHandler<Void>
{
    private final OperationsJobManager jobManager;
    @Inject
    public OperationsJobsHandler(InstanceMetadataFetcher metadataFetcher,
                                 ExecutorPools executorPools,
                                 CassandraInputValidator validator,
                                 OperationsJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    public void handleInternal(RoutingContext context, HttpServerRequest httpRequest, String host, SocketAddress remoteAddress, Void request)
    {
        UUID jobUUID = validateJobIdParam(context);

        executorPools.service().executeBlocking(() -> {
                         OperationsJob job = jobManager.getJobIfExists(jobUUID);
                         if (job == null)
                         {
                             String response = String.format("Unknown job with ID:%s. Please retry the operation.", jobUUID);
                             logger.info(response);
                             throw wrapHttpException(HttpResponseStatus.NOT_FOUND, response);
                         }
                         return job;
                     })
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request))
                     .onSuccess(job -> sendStatusBasedResponse(context, jobUUID, job));
    }

    private UUID validateJobIdParam(RoutingContext context)
    {
        String requestJobId = context.pathParam(OPERATIONS_JOB_ID_PATH_PARAM.substring(1));
        if (requestJobId == null)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    OPERATIONS_JOB_ID_PATH_PARAM + " is required but not supplied");
        }

        UUID jobId;
        try
        {
            jobId = UUID.fromString(requestJobId);
        }
        catch (IllegalArgumentException e)
        {
            String response = String.format("Invalid job ID provided :%s.", requestJobId);
            logger.info(response);
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, response);
        }
        return jobId;
    }

    public void sendStatusBasedResponse(RoutingContext context, UUID jobId, OperationsJob job)
    {

        if (job.status().isComplete())
        {
            context.response().setStatusCode(HttpResponseStatus.OK.code());
            OperationsJobResult.OperationsJobStatus status;
            final String reason;
            if (job.status().failed())
            {
                status = FAILED;
                reason = job.status().cause().getMessage();
            }
            else
            {
                status = COMPLETED;
                reason = "";
            }
            context.json(new OperationsJobsResponse(jobId, status, job.operation(), reason));
        }
        else
        {
            context.response()
                   .setStatusCode(HttpResponseStatus.ACCEPTED.code())
                   .putHeader(OPERATIONS_JOB_HEADER_NAME, jobId.toString())
                   .end();
        }
    }
}
