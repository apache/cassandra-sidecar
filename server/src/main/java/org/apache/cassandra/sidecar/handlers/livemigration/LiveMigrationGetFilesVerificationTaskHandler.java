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
 * Handler for retrieving a specific live migration files verification task by task ID.
 * Returns the task details if found, or a 404 error if the task does not exist on the specified host.
 */
@Singleton
public class LiveMigrationGetFilesVerificationTaskHandler extends AbstractHandler<String> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationGetFilesVerificationTaskHandler.class);

    private final FilesVerificationTaskManager taskManager;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    public LiveMigrationGetFilesVerificationTaskHandler(InstanceMetadataFetcher metadataFetcher,
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
        if (taskId == null || taskId.isEmpty())
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
            LiveMigrationTask<LiveMigrationFilesVerificationResponse> task = taskManager.getTask(taskId, host);
            LOGGER.debug("Found live migration task with taskId={} on host={}", taskId, host);
            context.json(task.getResponse());
        }
        catch (LiveMigrationTaskNotFoundException e)
        {
            LOGGER.warn("Live migration task not found with taskId={} on host={}", taskId, host);
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, e.getMessage(), e));
        }
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(DATA_COPY.toAuthorization());
    }
}
