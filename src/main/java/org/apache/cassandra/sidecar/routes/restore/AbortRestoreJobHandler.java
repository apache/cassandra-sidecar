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

package org.apache.cassandra.sidecar.routes.restore;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.request.data.AbortRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.metrics.RestoreMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API for aborting existing restore job maintained by Sidecar. Triggers abort status on the
 * {@link org.apache.cassandra.sidecar.db.RestoreJob}
 */
@Singleton
public class AbortRestoreJobHandler extends AbstractHandler<AbortRestoreJobRequestPayload>
{
    private static final AbortRestoreJobRequestPayload EMPTY_PAYLOAD = new AbortRestoreJobRequestPayload(null);

    private final RestoreJobDatabaseAccessor restoreJobDatabaseAccessor;
    private final RestoreMetrics metrics;

    @Inject
    public AbortRestoreJobHandler(ExecutorPools executorPools,
                                  InstanceMetadataFetcher instanceMetadataFetcher,
                                  RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                                  CassandraInputValidator validator,
                                  SidecarMetrics metrics)
    {
        super(instanceMetadataFetcher, executorPools, validator);
        this.restoreJobDatabaseAccessor = restoreJobDatabaseAccessor;
        this.metrics = metrics.server().restore();
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  AbortRestoreJobRequestPayload payload)
    {
        RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .map(job -> {
            if (job.status.isFinal())
            {
                throw wrapHttpException(HttpResponseStatus.CONFLICT,
                                        "Job is already in final state: " + job.status);
            }

            restoreJobDatabaseAccessor.abort(job.jobId, payload.reason());
            logger.info("Successfully aborted restore job. job={} remoteAddress={} instance={} reason='{}'",
                        job, remoteAddress, host, payload.reason());
            return job;
        })
        .onSuccess(job -> {
            metrics.failedJobs.metric.update(1);
            context.response().setStatusCode(HttpResponseStatus.OK.code()).end();
        })
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, payload));
    }

    @NotNull
    @Override
    protected AbortRestoreJobRequestPayload extractParamsOrThrow(RoutingContext context)
    {
        String bodyString = context.body().asString(); // nullable

        try
        {
            return Json.decodeValue(bodyString, AbortRestoreJobRequestPayload.class);
        }
        catch (Exception cause)
        {
            if (bodyString != null)
            {
                logger.warn("Failed to deserialize json string into AbortRestoreJobRequestPayload", cause);
            }
            return EMPTY_PAYLOAD;
        }
    }
}
