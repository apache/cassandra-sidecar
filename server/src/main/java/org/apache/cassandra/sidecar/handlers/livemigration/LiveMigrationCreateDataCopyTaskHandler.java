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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationDataCopyInProgressException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.DataCopyTaskManager;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTask;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for creating data copy tasks for live migration between Cassandra instances.
 * <p>
 * This handler processes requests to initiate data copying from a source Cassandra instance to a destination instance.
 * Data copy tasks must be submitted to the destination Sidecar instance. When a task is accepted,
 * this handler responds with HTTP 202 ACCEPTED status and returns a JSON response containing:
 * - taskId: Unique identifier for tracking the created task
 * - statusUrl: URL that can be used to query the status of the data copy operation
 */
public class LiveMigrationCreateDataCopyTaskHandler extends AbstractHandler<LiveMigrationDataCopyRequest> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationCreateDataCopyTaskHandler.class);
    private final DataCopyTaskManager dataCopyTaskManager;

    @Inject
    public LiveMigrationCreateDataCopyTaskHandler(InstanceMetadataFetcher metadataFetcher,
                                                  ExecutorPools executorPools,
                                                  CassandraInputValidator validator,
                                                  DataCopyTaskManager dataCopyTaskManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.dataCopyTaskManager = dataCopyTaskManager;
    }

    @Override
    protected LiveMigrationDataCopyRequest extractParamsOrThrow(RoutingContext context)
    {
        try
        {
            return Json.decodeValue(context.body().buffer(), LiveMigrationDataCopyRequest.class);
        }
        catch (DecodeException decodeException)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Error while decoding values, please check your request body.",
                                    decodeException);
        }
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  LiveMigrationDataCopyRequest liveMigrationTaskRequest)
    {
        dataCopyTaskManager
        .createTask(liveMigrationTaskRequest, host)
        .onSuccess(task -> {
            context.response().setStatusCode(HttpResponseStatus.ACCEPTED.code());
            context.json(buildResponse(task));
        })
        .onFailure(throwable -> {
            if (throwable instanceof LiveMigrationInvalidRequestException)
            {
                LOGGER.error("Input payload is not valid.", throwable);
                context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, throwable.getMessage(), throwable));
            }
            else if (throwable instanceof LiveMigrationDataCopyInProgressException)
            {
                LOGGER.error("Cannot start a new data copy task while another one is in progress.");
                context.fail(wrapHttpException(HttpResponseStatus.FORBIDDEN, throwable.getMessage(), throwable));
            }
            else
            {
                LOGGER.error("Unexpected exception while creating the data copy task.", throwable);
                context.fail(wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                               throwable.getMessage(), throwable));
            }
        });
    }

    private JsonObject buildResponse(LiveMigrationTask task)
    {
        return new JsonObject()
               .put("taskId", task.id())
               .put("statusUrl", ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE + "/" + task.id());
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.STREAM_FILES.toAuthorization());
    }
}
