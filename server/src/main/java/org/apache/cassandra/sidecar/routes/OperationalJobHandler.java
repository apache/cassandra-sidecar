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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.job.OperationalJob;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.OperationalJobUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.OPERATIONAL_JOB_ID_PATH_PARAM;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for retrieving the status of async operational jobs running on the sidecar
 */
public class OperationalJobHandler extends AbstractHandler<UUID> implements AccessProtected
{
    private final OperationalJobManager jobManager;

    @Inject
    public OperationalJobHandler(InstanceMetadataFetcher metadataFetcher,
                                 ExecutorPools executorPools,
                                 CassandraInputValidator validator,
                                 OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        String resource = VariableAwareResource.OPERATION.resource();
        return Collections.singleton(BasicPermissions.READ_OPERATIONAL_JOB.toAuthorization(resource));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               UUID jobId)
    {
        executorPools.service()
                     .executeBlocking(() -> {
                         OperationalJob job = jobManager.getJobIfExists(jobId);
                         if (job == null)
                         {
                             logger.info("No operational job found with the jobId. jobId={}", jobId);
                             throw wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                                     String.format("Unknown job with ID: %s. Please retry the operation.", jobId));
                         }
                         return job;
                     })
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, jobId))
                     .onSuccess(job -> OperationalJobUtils.sendStatusBasedResponse(context, job));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UUID extractParamsOrThrow(RoutingContext context)
    {
        return validatedJobIdParam(context);
    }

    private UUID validatedJobIdParam(RoutingContext context)
    {
        String requestJobId = context.pathParam(OPERATIONAL_JOB_ID_PATH_PARAM.substring(1));
        if (requestJobId == null)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    OPERATIONAL_JOB_ID_PATH_PARAM + " is required but not supplied");
        }

        UUID jobId;
        try
        {
            jobId = UUID.fromString(requestJobId);
        }
        catch (IllegalArgumentException e)
        {
            logger.info("Invalid jobId. jobId={}", requestJobId);
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    String.format("Invalid job ID provided: %s.", requestJobId));
        }
        return jobId;
    }
}
