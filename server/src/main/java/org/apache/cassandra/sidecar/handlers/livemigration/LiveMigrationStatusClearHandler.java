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

import java.io.IOException;
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
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationStatusTracker;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler to clear the completed live migration status for an instance.
 * Allows the instance to be migrated again in the future by removing the completion status.
 * Should only be called after the instance entry is removed from the Live Migration map.
 */
public class LiveMigrationStatusClearHandler extends AbstractHandler<Void> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationStatusClearHandler.class);

    private final LiveMigrationStatusTracker statusTracker;

    @Inject
    public LiveMigrationStatusClearHandler(InstanceMetadataFetcher metadataFetcher,
                                           ExecutorPools executorPools,
                                           CassandraInputValidator validator,
                                           LiveMigrationStatusTracker statusTracker)
    {
        super(metadataFetcher, executorPools, validator);
        this.statusTracker = statusTracker;
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext context, HttpServerRequest httpRequest, @NotNull String host, SocketAddress remoteAddress, Void request)
    {
        InstanceMetadata instanceMetadata = metadataFetcher.instance(host);
        executorPools.service().runBlocking(() -> {
            try
            {
                statusTracker.unsetMigrationCompleted(instanceMetadata);
                context.response().setStatusCode(HttpResponseStatus.OK.code()).end();
            }
            catch (IllegalArgumentException iae)
            {
                LOGGER.error("Error while clearing live migration status for instance {}", host, iae);
                context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, iae.getMessage(), iae));
            }
            catch (IOException e)
            {
                LOGGER.error("Error while clearing live migration status for instance {}", host, e);
                context.fail(wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, e.getMessage(), e));
            }
        });
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.STREAM_FILES.toAuthorization());
    }
}
