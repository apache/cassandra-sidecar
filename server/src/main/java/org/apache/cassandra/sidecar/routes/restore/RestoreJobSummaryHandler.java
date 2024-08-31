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

import javax.inject.Inject;
import javax.inject.Singleton;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API for providing summary of restore job maintained by Sidecar
 */
@Singleton
public class RestoreJobSummaryHandler extends AbstractHandler<String>
{
    @Inject
    public RestoreJobSummaryHandler(ExecutorPools executorPools,
                                    InstanceMetadataFetcher instanceMetadataFetcher,
                                    CassandraInputValidator validator)
    {
        super(instanceMetadataFetcher, executorPools, validator);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  String jobId)
    {
        validateAndFindJob(context)
        .onSuccess(context::json)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, jobId));
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.pathParam("jobId");
    }

    private Future<RestoreJobSummaryResponsePayload> validateAndFindJob(RoutingContext context)
    {
        return RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .compose(restoreJob -> {
            if (restoreJob.status == null || restoreJob.secrets == null)
            {
                logger.error("Restore job record read is missing required fields. job={}", restoreJob);
                return Future.failedFuture(wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                             "Restore job is missing required fields"));
            }
            RestoreJobSummaryResponsePayload response
            = new RestoreJobSummaryResponsePayload(restoreJob.createdAt.toString(), restoreJob.jobId,
                                                   restoreJob.jobAgent, restoreJob.keyspaceName, restoreJob.tableName,
                                                   restoreJob.secrets, restoreJob.statusWithOptionalDescription());
            return Future.succeededFuture(response);
        });
    }
}
