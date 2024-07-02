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

import java.nio.file.Paths;
import javax.inject.Inject;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.restore.RestoreJobManagerGroup;
import org.apache.cassandra.sidecar.restore.RestoreJobProgressTracker;
import org.apache.cassandra.sidecar.restore.RestoreJobUtil;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_QUALIFIED_TABLE_NAME;
import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API for creating new {@link RestoreSlice} under a {@link RestoreJob}
 */
public class CreateRestoreSliceHandler extends AbstractHandler<CreateSliceRequestPayload>
{
    private static final int SERVER_ERROR_RESTORE_JOB_FAILED = 550;
    private final RestoreJobManagerGroup restoreJobManagerGroup;
    private final RestoreSliceDatabaseAccessor restoreSliceDatabaseAccessor;

    @Inject
    public CreateRestoreSliceHandler(ExecutorPools executorPools,
                                     InstanceMetadataFetcher instanceMetadataFetcher,
                                     RestoreJobManagerGroup restoreJobManagerGroup,
                                     RestoreSliceDatabaseAccessor restoreSliceDatabaseAccessor,
                                     CassandraInputValidator validator)
    {
        super(instanceMetadataFetcher, executorPools, validator);
        this.restoreJobManagerGroup = restoreJobManagerGroup;
        this.restoreSliceDatabaseAccessor = restoreSliceDatabaseAccessor;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  CreateSliceRequestPayload request)
    {
        InstanceMetadata instance = metadataFetcher.instance(host);
        RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .map(restoreJob -> {
            // the job is either aborted or succeeded
            if (restoreJob.status.isFinal())
            {
                logger.debug("The job has completed already. job={}", restoreJob);
                // prevent creating slice, since the job is already in the final state
                String errMsg = "Job is already in final state: " + restoreJob.status;
                throw wrapHttpException(HttpResponseStatus.CONFLICT, errMsg);
            }
            return restoreJob;
        })
        .compose(restoreJob -> RoutingContextUtils.getAsFuture(context, SC_QUALIFIED_TABLE_NAME).map(tableName -> {
            RestoreSlice slice = RestoreSlice
                                 .builder()
                                 .jobId(restoreJob.jobId)
                                 .qualifiedTableName(tableName)
                                 .createSliceRequestPayload(request)
                                 .build();
            return new RestoreSliceAndJob(slice, restoreJob);
        }))
        .compose(sliceAndJob -> {
            // Send response back if all are good, and
            // it should catch whatever exception and handle at onFailure
            RestoreSlice slice = sliceAndJob.restoreSlice;
            RestoreJob job = sliceAndJob.restoreJob;

            if (job.isManagedBySidecar())
            {
                createSliceForSidecarManagedJob(context, slice);
            }
            else
            {
                createOrPollRangeForSparkManagedJob(context, instance, job, slice);
            }
            return Future.succeededFuture();
        })
        .onSuccess(nothing -> { // verify that the response should be ended if no error is thrown from prior steps
            if (!context.response().ended())
            {
                logger.warn("The response should have been ended on the absence of error, but not.");
                context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            }
        })
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected CreateSliceRequestPayload extractParamsOrThrow(RoutingContext context)
    {
        String bodyString = context.getBodyAsString();
        if (bodyString == null || bodyString.equalsIgnoreCase("null")) // json encoder writes null as "null"
        {
            logger.warn("Bad request to create restore slice. Received null payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Unexpected null payload for request");
        }
        try
        {
            return Json.decodeValue(bodyString, CreateSliceRequestPayload.class);
        }
        catch (DecodeException decodeException)
        {
            logger.warn("Bad request to create restore slice. Received invalid JSON payload. payload={}", bodyString);
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request payload", decodeException);
        }
    }

    private void createOrPollRangeForSparkManagedJob(RoutingContext context,
                                                     InstanceMetadata instance,
                                                     RestoreJob job,
                                                     RestoreSlice slice)
    {
        RestoreJobProgressTracker.Status status;
        String uploadId = RestoreJobUtil.generateUniqueUploadId(job.jobId, slice.sliceId());
        RestoreRange range = RestoreRange.builderFromSlice(slice)
                                         .ownerInstance(instance)
                                         .stageDirectory(Paths.get(instance.stagingDir()), uploadId)
                                         .build();

        try
        {
            status = restoreJobManagerGroup.trySubmit(instance, range, job);
        }
        catch (RestoreJobFatalException ex)
        {
            String errorMessage = "Restore slice failed. jobId=" + slice.jobId() + " sliceId=" + slice.sliceId();
            logger.error(errorMessage, ex);
            // propagate the restore slice failure message to client with custom server error status code
            context.fail(wrapHttpException(HttpResponseStatus.valueOf(SERVER_ERROR_RESTORE_JOB_FAILED),
                                           errorMessage,
                                           ex));
            return;
        }

        logger.info("slice is {}. slice key {}", status, slice.key());

        switch (status)
        {
            case CREATED:
                context.response()
                       .setStatusCode(HttpResponseStatus.CREATED.code())
                       .end();
                break;
            case PENDING:
                context.response()
                       .setStatusCode(HttpResponseStatus.ACCEPTED.code())
                       .end();
                break;
            case COMPLETED:
                context.response()
                       .setStatusCode(HttpResponseStatus.OK.code())
                       .end();
                break;
            default:
                logger.error("Unknown restore slice status. jobId={}, sliceId={}, status={}",
                             slice.jobId(), slice.sliceId(), status);
                context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                break;
        }
    }

    // For sidecar managed jobs, each slice is submitted/created once.
    // Unlike the spark managed job, the spark driver calls the same endpoint to poll restore status
    private void createSliceForSidecarManagedJob(RoutingContext context, RestoreSlice slice)
    {
        try
        {
            restoreSliceDatabaseAccessor.create(slice);
        }
        catch (Exception ex)
        {
            logger.error("Failed to persist restore slice. jobId={} sliceId={}",
                         slice.jobId(), slice.sliceId(), ex);
            // todo: verify client can retry on this error
            context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            return;
        }

        context.response().setStatusCode(HttpResponseStatus.CREATED.code()).end();
    }

    private static class RestoreSliceAndJob
    {
        final RestoreJob restoreJob;
        final RestoreSlice restoreSlice;

        RestoreSliceAndJob(RestoreSlice restoreSlice, RestoreJob restoreJob)
        {
            this.restoreJob = restoreJob;
            this.restoreSlice = restoreSlice;
        }
    }
}
