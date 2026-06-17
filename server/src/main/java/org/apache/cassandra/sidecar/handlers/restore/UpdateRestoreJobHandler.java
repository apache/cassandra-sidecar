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

package org.apache.cassandra.sidecar.handlers.restore;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.data.CredentialType;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.RestoreMetrics;
import org.apache.cassandra.sidecar.restore.RestoreJobDiscoverer;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API to update {@link RestoreJob}
 */
@Singleton
public class UpdateRestoreJobHandler extends AbstractHandler<UpdateRestoreJobRequestPayload> implements AccessProtected
{
    private final RestoreJobDatabaseAccessor restoreJobDatabaseAccessor;
    private final RestoreJobDiscoverer restoreJobDiscoverer;
    private final RestoreMetrics metrics;

    @Inject
    public UpdateRestoreJobHandler(ExecutorPools executorPools,
                                   InstanceMetadataFetcher instanceMetadataFetcher,
                                   RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                                   RestoreJobDiscoverer restoreJobDiscoverer,
                                   CassandraInputValidator validator,
                                   SidecarMetrics metrics)
    {
        super(instanceMetadataFetcher, executorPools, validator);
        this.restoreJobDatabaseAccessor = restoreJobDatabaseAccessor;
        this.restoreJobDiscoverer = restoreJobDiscoverer;
        this.metrics = metrics.server().restore();
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.EDIT_RESTORE_JOB.toAuthorization());
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  UpdateRestoreJobRequestPayload requestPayload)
    {
        RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .compose(existingJob -> {
            if (existingJob.status.isFinal())
            {
                // skip the update, since the job is in the final state already
                logger.debug("The job has completed already. job={}", existingJob);
                return Future.failedFuture(wrapHttpException(HttpResponseStatus.CONFLICT,
                                                             "Job is already in final state: " + existingJob.status));
            }

            // IAM jobs derive credentials from the instance profile / task role at runtime.
            // There are no static credentials to supply or rotate, so any attempt to update
            // credentials on an IAM job is always invalid.
            if (existingJob.credentialType == CredentialType.IAM && requestPayload.secrets() != null)
            {
                logger.warn("Credential update rejected for IAM job. job={}", existingJob);
                return Future.failedFuture(wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                                             "Credentials cannot be updated for IAM jobs"));
            }

            return executorPools.service()
                                .executeBlocking(() -> restoreJobDatabaseAccessor.update(requestPayload, existingJob));
        })
        .onSuccess(updatedJob -> {
            logger.info("Successfully updated restore job. job={}, request={}, remoteAddress={}, instance={}",
                        updatedJob, requestPayload, remoteAddress, host);
            if (updatedJob.status == RestoreJobStatus.SUCCEEDED)
            {
                metrics.successfulJobs.metric.update(1);
                long startMillis = UUIDs.unixTimestamp(updatedJob.jobId);
                long durationMillis = System.currentTimeMillis() - startMillis;
                // toNanos does not overflow. Nanos in `long` can at most represent 106,751 days.
                metrics.jobCompletionTime.metric.update(durationMillis, TimeUnit.MILLISECONDS);
            }

            if (updatedJob.secrets != null)
            {
                metrics.tokenRefreshed.metric.update(1);
            }

            context.response().setStatusCode(HttpResponseStatus.OK.code()).end();
            // Fire-and-forget on the internal worker pool — notifying the restore system is internal work
            // that shouldn't block the event loop or share the service pool with client-facing requests.
            executorPools.internal().runBlocking(() -> notifyPhaseSignalMaybe(updatedJob));
        })
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, requestPayload));
    }

    @Override
    protected UpdateRestoreJobRequestPayload extractParamsOrThrow(RoutingContext context)
    {
        String bodyString = context.body().asString();
        if (bodyString == null || bodyString.equalsIgnoreCase("null")) // json encoder writes null as "null"
        {
            logger.warn("Bad request to update restore job. Received null payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Unexpected null payload for request");
        }
        try
        {
            UpdateRestoreJobRequestPayload payload = Json.decodeValue(bodyString, UpdateRestoreJobRequestPayload.class);
            if (payload.isEmpty())
            {
                logger.warn("Bad request to update restore job. Received empty payload.");
                throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                        "Update request body cannot have all empty fields");
            }
            return payload;
        }
        catch (DecodeException decodeException)
        {
            logger.warn("Bad request to update restore job. Received invalid JSON payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request payload", decodeException);
        }
    }

    private void notifyPhaseSignalMaybe(RestoreJob updatedJob)
    {
        if (updatedJob.status != RestoreJobStatus.IMPORT_READY && updatedJob.status != RestoreJobStatus.STAGE_READY)
        {
            return;
        }

        try
        {
            restoreJobDiscoverer.processJobNow(updatedJob);
        }
        catch (Exception e)
        {
            logger.warn("Failed to immediately process phase signal. " +
                        "The discovery loop will pick it up on the next cycle. jobId={}", updatedJob.jobId, e);
        }
    }
}
