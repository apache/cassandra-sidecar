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
import org.apache.cassandra.sidecar.common.response.JobStatusResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.job.Job;
import org.apache.cassandra.sidecar.job.JobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.ASYNC_JOB_UUID;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for retrieving the status of async jobs running on the sidecar
 */
public class JobStatusHandler extends AbstractHandler<Void>
{
    private final JobManager jobManager;
    @Inject
    public JobStatusHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools, CassandraInputValidator validator, JobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
    }

    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    public void handleInternal(RoutingContext context, HttpServerRequest httpRequest, String host, SocketAddress remoteAddress, Void request)
    {

        String jobId = context.pathParam("jobId");
        if (jobId == null)
        {
            context.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
        }

        Job job = jobManager.getJobIfExists(jobId);
        if (job == null)
        {
            String response = String.format("Unknown job with ID:%s. Please retry the operation.", jobId);
            logger.info(response);
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, response));
            return;
        }

        executorPools.service()
                     .runBlocking(() -> {
                         sendStatusBasedResponse(context, jobId, job);
                     })
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    public void sendStatusBasedResponse(RoutingContext context, String jobId, Job job)
    {
        switch(job.status())
        {
            case Completed:
                context.response().setStatusCode(HttpResponseStatus.OK.code());
                context.json(new JobStatusResponse(UUID.fromString(jobId), job.status(), job.operation()));
                break;
            case Failed:
                context.fail(wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, job.failureReason()));
                break;
            case Pending:
            case Running:
                context.response()
                       .setStatusCode(HttpResponseStatus.ACCEPTED.code())
                       .putHeader(ASYNC_JOB_UUID, jobId)
                       .end();
                break;
        }
    }
}
