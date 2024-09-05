/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.routes.restore;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.restore.RestoreJobConsistencyLevelChecker;
import org.apache.cassandra.sidecar.restore.RestoreJobProgress;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API for querying the progress of a {@link RestoreJob}
 * The response content can vary based on the {@link RestoreJobProgressFetchPolicy}
 */
@Singleton
public class RestoreJobProgressHandler extends AbstractHandler<RestoreJobProgressFetchPolicy>
{
    private final RestoreJobConsistencyLevelChecker consistencyLevelChecker;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    public RestoreJobProgressHandler(InstanceMetadataFetcher metadataFetcher,
                                     ExecutorPools executorPools,
                                     CassandraInputValidator validator,
                                     RestoreJobConsistencyLevelChecker consistencyLevelChecker)
    {
        super(metadataFetcher, executorPools, validator);
        this.consistencyLevelChecker = consistencyLevelChecker;
    }

    @Override
    protected RestoreJobProgressFetchPolicy extractParamsOrThrow(RoutingContext context)
    {
        List<String> fetchPolicyValues = context.queryParam(ApiEndpointsV1.FETCH_POLICY_QUERY_PARAM);
        if (fetchPolicyValues.isEmpty())
        {
            logger.info("No RestoreJobProgressFetchPolicy is specified, FIRST_FAILED policy is assumed");
            return RestoreJobProgressFetchPolicy.FIRST_FAILED;
        }
        else if (fetchPolicyValues.size() > 1)
        {
            logger.warn("Multiple RestoreJobProgressFetchPolicy are specified. Pick the first one.");
        }
        return RestoreJobProgressFetchPolicy.fromString(fetchPolicyValues.get(0));
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  RestoreJobProgressFetchPolicy fetchPolicy)
    {
        RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .map(this::ensureRestoreJobIsManagedBySidecar)
        .compose(restoreJob -> consistencyLevelChecker.check(restoreJob, fetchPolicy))
        .map(RestoreJobProgress::toResponsePayload)
        .onSuccess(context::json)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, fetchPolicy));
    }

    private RestoreJob ensureRestoreJobIsManagedBySidecar(RestoreJob restoreJob)
    {
        if (!restoreJob.isManagedBySidecar())
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Only Sidecar-managed restore jobs are allowed. " +
                                    "jobId=" + restoreJob.jobId +
                                    " jobManager=" + restoreJob.restoreJobManager.name());
        }
        return restoreJob;
    }
}
