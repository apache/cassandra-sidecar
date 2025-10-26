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
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskNotFoundException;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.DataCopyTaskManager;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTask;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler that retrieves the status and details of a specific data copy task in the live migration process.
 * This handler processes GET requests for a particular live migration task by its unique task ID.
 */
public class LiveMigrationGetDataCopyTaskHandler extends AbstractHandler<String> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationGetDataCopyTaskHandler.class);
    private final DataCopyTaskManager dataCopyTaskManager;

    @Inject
    public LiveMigrationGetDataCopyTaskHandler(InstanceMetadataFetcher metadataFetcher,
                                               ExecutorPools executorPools,
                                               CassandraInputValidator validator,
                                               DataCopyTaskManager dataCopyTaskManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.dataCopyTaskManager = dataCopyTaskManager;
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.pathParam("taskId");
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
            LiveMigrationTask task = dataCopyTaskManager.getTask(taskId, host);
            LOGGER.info("Found the live migration task with id={}", taskId);
            context.json(task.getResponse());
        }
        catch (LiveMigrationTaskNotFoundException e)
        {
            LOGGER.warn("Could not find live migration data copy task with id={}", taskId);
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, e.getMessage(), e));
        }
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.DATA_COPY.toAuthorization());
    }
}
