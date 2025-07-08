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
 * Handler for cancelling live migration data copy tasks.
 */
public class LiveMigrationCancelDataCopyTaskHandler extends AbstractHandler<String> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationCancelDataCopyTaskHandler.class);
    private final DataCopyTaskManager dataCopyTaskManager;

    @Inject
    public LiveMigrationCancelDataCopyTaskHandler(InstanceMetadataFetcher metadataFetcher,
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
            LiveMigrationTask task = dataCopyTaskManager.cancelTask(taskId, host);
            LOGGER.info("Successfully cancelled the data copy task with TaskID={}", taskId);
            context.json(task.getResponse());
        }
        catch (LiveMigrationTaskNotFoundException e)
        {
            LOGGER.warn("Live migration data copy task not found for cancellation. TaskID={} Host={}", taskId, host);
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, e.getMessage(), e));
        }
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.STREAM_FILES.toAuthorization());
    }
}
