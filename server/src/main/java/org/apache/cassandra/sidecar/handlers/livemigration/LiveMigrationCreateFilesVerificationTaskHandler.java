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
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskCreationResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationTaskInProgressException;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.FilesVerificationTaskManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DATA_COPY;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * HTTP handler for creating file digest verification tasks during live migration.
 * Manages concurrent verification tasks per instance and orchestrates the verification process.
 */
@Singleton
public class LiveMigrationCreateFilesVerificationTaskHandler extends AbstractHandler<LiveMigrationFilesVerificationRequest> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationCreateFilesVerificationTaskHandler.class);

    private final FilesVerificationTaskManager filesVerificationTaskManager;
    private final LiveMigrationMap liveMigrationMap;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected LiveMigrationCreateFilesVerificationTaskHandler(InstanceMetadataFetcher metadataFetcher,
                                                              ExecutorPools executorPools,
                                                              CassandraInputValidator validator,
                                                              LiveMigrationMap liveMigrationMap,
                                                              FilesVerificationTaskManager filesVerificationTaskManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.liveMigrationMap = liveMigrationMap;
        this.filesVerificationTaskManager = filesVerificationTaskManager;
    }

    @Override
    protected LiveMigrationFilesVerificationRequest extractParamsOrThrow(RoutingContext context)
    {
        try
        {
            return Json.decodeValue(context.body().buffer(), LiveMigrationFilesVerificationRequest.class);
        }
        catch (DecodeException decodeException)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Failed to parse request body, please ensure that the request is valid.",
                                    decodeException);
        }
        catch (IllegalArgumentException e)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  LiveMigrationFilesVerificationRequest request)
    {
        InstanceMetadata localInstanceMetadata;
        try
        {
            localInstanceMetadata = metadataFetcher.instance(host);
        }
        catch (NoSuchCassandraInstanceException e)
        {
            LOGGER.error("Failed to fetch instance metadata for host={}", host);
            context.fail(wrapHttpException(SERVICE_UNAVAILABLE, e));
            return;
        }

        liveMigrationMap.getSource(host)
                        .compose(source -> filesVerificationTaskManager.createTask(request, source, localInstanceMetadata))

                        .onSuccess(task -> {
                            LOGGER.info("Created files verification task {} for host {}", task.id(), host);
                            context.response().setStatusCode(ACCEPTED.code());
                            String statusUrl = LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE + "/" + task.id();
                            context.json(new LiveMigrationTaskCreationResponse(task.id(), statusUrl));
                        })

                        .onFailure(throwable -> {
                            if (throwable instanceof LiveMigrationTaskInProgressException)
                            {
                                LOGGER.error("Cannot start a new files verification task for host {} " +
                                             "while another live migration task is in progress.", host);
                                context.fail(wrapHttpException(CONFLICT, throwable.getMessage(), throwable));
                                return;
                            }
                            else if (throwable instanceof LiveMigrationInvalidRequestException)
                            {
                                LOGGER.error("Invalid request {}", request, throwable);
                                context.fail(wrapHttpException(BAD_REQUEST, throwable.getMessage(), throwable));
                                return;
                            }
                            LOGGER.error("Failed to create files verification task for host {}.", host, throwable);
                            context.fail(wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                           throwable.getMessage(), throwable));
                        });
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(DATA_COPY.toAuthorization());
    }
}
