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

import java.util.UUID;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.RestoreJobConstants;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.RESTORE_JOBS;
import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_QUALIFIED_TABLE_NAME;
import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for validating various facts about requests related to restore functionality of Sidecar
 */
@Singleton
public class RestoreRequestValidationHandler implements Handler<RoutingContext>
{
    private final CassandraInputValidator validator;
    private final RestoreJobDatabaseAccessor restoreJobs;
    private final ExecutorPools executorPools;

    @Inject
    public RestoreRequestValidationHandler(CassandraInputValidator validator,
                                           RestoreJobDatabaseAccessor restoreJobs,
                                           ExecutorPools executorPools)
    {
        this.validator = validator;
        this.restoreJobs = restoreJobs;
        this.executorPools = executorPools;
    }

    @Override
    public void handle(RoutingContext context)
    {
        QualifiedTableName tableName = verifyQualifiedTableName(context);
        RoutingContextUtils.put(context, SC_QUALIFIED_TABLE_NAME, tableName);
        UUID jobId = verifyJobIdIfPresent(context);
        verifyJobStatusIfPresent(context);
        verifyJobExistAndStoreAsync(context, jobId, tableName)
        .onSuccess(ignored -> context.next())
        .onFailure(context::fail);
    }

    private QualifiedTableName verifyQualifiedTableName(RoutingContext context)
    {
        String keyspace = context.pathParam("keyspace");
        String table = context.pathParam("table");
        validator.validateKeyspaceName(keyspace);
        validator.validateTableName(table);
        return new QualifiedTableName(keyspace, table, true);
    }

    private UUID verifyJobIdIfPresent(RoutingContext context)
    {
        HttpServerRequest request = context.request();
        // Skip jobId verification if the request is sent to create restore job endpoint
        if (request.method() == HttpMethod.POST
            && request.path().endsWith(RESTORE_JOBS))
        {
            return null;
        }

        String jobIdFromPath = context.pathParam("jobId");
        if (jobIdFromPath == null)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "PathParam jobId must be present for the request");
        }

        UUID jobId;
        try
        {
            jobId = UUID.fromString(jobIdFromPath);
        }
        catch (Exception e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Invalid jobId - not an UUID, " + jobIdFromPath);
        }
        if (jobId.version() != 1) // Version number 1: Time-based UUID
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Invalid jobId - not a time-based UUID, " + jobIdFromPath);
        }
        return jobId;
    }

    private Future<Void> verifyJobExistAndStoreAsync(RoutingContext context, @Nullable UUID jobId,
                                                     QualifiedTableName tableName)
    {
        if (jobId == null)
        {
            return Future.succeededFuture();
        }
        return findJob(jobId)
               .compose(restoreJob -> {
                   // Make sure the persisted restore job has the matching keyspace and table values
                   // It is to avoid selecting a wrong job mistakenly
                   if (!tableName.keyspace().equalsIgnoreCase(restoreJob.keyspaceName)
                       || !tableName.tableName().equalsIgnoreCase(restoreJob.tableName))
                   {
                       return Future.failedFuture(wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                                                    "Restore job with jobId: " + jobId +
                                                                    " is not found for table: " + tableName));
                   }
                   RoutingContextUtils.put(context, SC_RESTORE_JOB, restoreJob);
                   return Future.succeededFuture();
               });
    }

    private void verifyJobStatusIfPresent(RoutingContext context)
    {
        String status = context.getBodyAsJson() == null
                        ? null
                        : context.getBodyAsJson().getString(RestoreJobConstants.JOB_STATUS);
        if (status == null)
        {
            return;
        }
        try
        {
            RestoreJobStatus.valueOf(status);
        }
        catch (Exception e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Unrecognized restore job status passed, " + status);
        }
    }

    private Future<RestoreJob> findJob(UUID jobId)
    {
        return executorPools.service().executeBlocking(() -> {
            RestoreJob restoreJob = restoreJobs.find(jobId);
            if (restoreJob == null)
            {
                throw wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                        "Restore job with id: " + jobId + " does not exist");
            }

            return restoreJob;
        });
    }
}
