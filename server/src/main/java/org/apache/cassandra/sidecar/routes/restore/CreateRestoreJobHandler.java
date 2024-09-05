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
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.response.data.CreateRestoreJobResponsePayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_QUALIFIED_TABLE_NAME;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides REST API for creating a new restore job for restoring data into Cassandra through Sidecar
 */
@Singleton
public class CreateRestoreJobHandler extends AbstractHandler<CreateRestoreJobRequestPayload>
{
    private final RestoreJobDatabaseAccessor restoreJobDatabaseAccessor;

    @Inject
    public CreateRestoreJobHandler(ExecutorPools executorPools,
                                   InstanceMetadataFetcher instanceMetadataFetcher,
                                   RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                                   CassandraInputValidator validator)
    {
        super(instanceMetadataFetcher, executorPools, validator);
        this.restoreJobDatabaseAccessor = restoreJobDatabaseAccessor;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  CreateRestoreJobRequestPayload request)
    {
        validatePayload(request)
        .compose(payload -> createRestoreJob(context, payload))
        .onSuccess(createdJob -> {
            logger.info("Successfully persisted a new job. job={} request={} remoteAddress={} instance={}",
                        createdJob, request, remoteAddress, host);
            context.response().setStatusCode(HttpResponseStatus.OK.code());
            context.json(new CreateRestoreJobResponsePayload(createdJob.jobId, createdJob.status.name()));
        })
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected CreateRestoreJobRequestPayload extractParamsOrThrow(RoutingContext context)
    {
        String bodyString = context.body().asString();
        if (bodyString == null || bodyString.equalsIgnoreCase("null")) // json encoder writes null as "null"
        {
            logger.warn("Bad request to create restore job. Received null payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Unexpected null payload for request");
        }

        try
        {
            return Json.decodeValue(bodyString, CreateRestoreJobRequestPayload.class);
        }
        catch (DecodeException decodeException)
        {
            // do not log the payload as it contains the secrets
            logger.warn("Bad request to create restore job. Received invalid JSON payload.");
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Invalid request payload",
                                    decodeException);
        }
    }

    private Future<CreateRestoreJobRequestPayload> validatePayload(CreateRestoreJobRequestPayload
                                                                   createRestoreJobRequestPayload)
    {
        UUID jobId = createRestoreJobRequestPayload.jobId();
        if (jobId == null)
        {
            return Future.succeededFuture(createRestoreJobRequestPayload);
        }

        return executorPools.service().executeBlocking(() -> {
            if (restoreJobDatabaseAccessor.exists(jobId))
            {
                logger.info("Restore job already exist. jobId={}", jobId);
                throw wrapHttpException(HttpResponseStatus.CONFLICT,
                                        String.format("Job id %s already exists", jobId));
            }

            return createRestoreJobRequestPayload;
        });
    }

    private Future<RestoreJob> createRestoreJob(RoutingContext context, CreateRestoreJobRequestPayload payload)
    {
        return RoutingContextUtils.getAsFuture(context, SC_QUALIFIED_TABLE_NAME)
                                  .compose(tableName -> executorPools.service().executeBlocking(() -> {
                                      return restoreJobDatabaseAccessor.create(payload, tableName);
                                  }));
    }
}
