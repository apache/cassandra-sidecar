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
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.restore.RestoreJobManagerGroup;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.stats.RestoreJobStats;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API for aborting existing restore job maintained by Sidecar. Triggers abort status on the
 * {@link org.apache.cassandra.sidecar.db.RestoreJob}
 */
@Singleton
public class AbortRestoreJobHandler extends AbstractHandler<String>
{
    private final RestoreJobDatabaseAccessor restoreJobDatabaseAccessor;
    private final RestoreJobManagerGroup restoreJobManagerGroup;
    private final RestoreJobStats stats;

    @Inject
    public AbortRestoreJobHandler(ExecutorPools executorPools,
                                  InstanceMetadataFetcher instanceMetadataFetcher,
                                  RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                                  RestoreJobManagerGroup restoreJobManagerGroup,
                                  CassandraInputValidator validator,
                                  RestoreJobStats stats)
    {
        super(instanceMetadataFetcher, executorPools, validator);
        this.restoreJobDatabaseAccessor = restoreJobDatabaseAccessor;
        this.restoreJobManagerGroup = restoreJobManagerGroup;
        this.stats = stats;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  String jobId)
    {
        RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .compose(job -> {
            if (RestoreJobStatus.isFinalState(job.status))
            {
                return Future.failedFuture(wrapHttpException(HttpResponseStatus.CONFLICT,
                                                             "Job is already in final state: " + job.status));
            }

            restoreJobDatabaseAccessor.abort(job.jobId);
            restoreJobManagerGroup.signalRefreshRestoreJob();
            return Future.succeededFuture(job);
        })
        .onSuccess(job -> {
            logger.info("Successfully aborted restore job. job={}, remoteAddress={}, instance={}",
                        job, remoteAddress, host);
            stats.captureFailedJob();
            context.response().setStatusCode(HttpResponseStatus.OK.code()).end();
        })
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, jobId));
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.pathParam("jobId");
    }
}
