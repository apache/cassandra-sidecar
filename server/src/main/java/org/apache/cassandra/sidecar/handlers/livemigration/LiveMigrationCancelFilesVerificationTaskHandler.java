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
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.FilesVerificationTaskManager;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTask;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DATA_COPY;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for canceling an active live migration files verification task.
 * Accepts a task ID and cancels the corresponding verification task on the specified host.
 */
@Singleton
public class LiveMigrationCancelFilesVerificationTaskHandler extends AbstractHandler<String> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationCancelFilesVerificationTaskHandler.class);

    private final FilesVerificationTaskManager taskManager;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    public LiveMigrationCancelFilesVerificationTaskHandler(InstanceMetadataFetcher metadataFetcher,
                                                           ExecutorPools executorPools,
                                                           CassandraInputValidator validator,
                                                           FilesVerificationTaskManager taskManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.taskManager = taskManager;
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        String taskId = context.pathParam("taskId");
        if (taskId == null || taskId.isBlank())
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "taskId is required");
        }
        return taskId;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  String taskId)
    {
        try
        {
            LiveMigrationTask<LiveMigrationFilesVerificationResponse> task = taskManager.cancelTask(taskId, host);
            LOGGER.info("Successfully cancelled the files verification task with taskId={} host={}", taskId, host);
            context.json(task.getResponse());
        }
        catch (LiveMigrationTaskNotFoundException e)
        {
            LOGGER.warn("Live migration files verification task not found for cancellation. " +
                        "taskId={} host={}", taskId, host);
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                           "No live migration task found with id " + taskId));
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to cancel files verification task. taskId={} host={}", taskId, host, e);
            context.fail(wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                           "Failed to cancel task: " + e.getMessage()));
        }
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(DATA_COPY.toAuthorization());
    }
}
