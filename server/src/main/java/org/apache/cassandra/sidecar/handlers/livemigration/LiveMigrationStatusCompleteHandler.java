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
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationStatusTracker;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler to safely mark a live migration as COMPLETED for an instance.
 * <p>
 * Marking live migration as COMPLETED provides safety from re-migrating instances that have
 * been migrated already. To ensure safety, other live migration endpoints such as file streaming
 * or data-copy requests will automatically stop serving to requests.
 *
 * <h2>Usage Safety:</h2>
 * This endpoint should only be called when the live migration process has genuinely completed
 * all data transfer and validation steps. Premature completion can lead to data inconsistency
 * or loss during cluster operations.
 *
 * @see LiveMigrationStatusTracker#hasMigrationCompleted(InstanceMetadata)
 */
@Singleton
public class LiveMigrationStatusCompleteHandler extends AbstractHandler<Void> implements AccessProtected
{

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationStatusCompleteHandler.class);

    private final LiveMigrationStatusTracker statusTracker;

    @Inject
    public LiveMigrationStatusCompleteHandler(InstanceMetadataFetcher metadataFetcher,
                                              ExecutorPools executorPools,
                                              CassandraInputValidator validator,
                                              LiveMigrationStatusTracker statusTracker)
    {
        super(metadataFetcher, executorPools, validator);
        this.statusTracker = statusTracker;
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext routingContext)
    {
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext routingContext, HttpServerRequest httpServerRequest,
                                  @NotNull String host, SocketAddress socketAddress, Void unused)
    {
        InstanceMetadata instanceMetadata = metadataFetcher.instance(host);

        statusTracker.hasMigrationCompleted(instanceMetadata)
                     .compose(completed -> {
                         if (completed)
                         {
                             LOGGER.error("Attempted to update live migration status as COMPLETED for instance {} that was " +
                                          "marked as COMPLETED already.", instanceMetadata.host());
                             return Future.failedFuture(new IllegalStateException("Live migration marked as COMPLETED earlier."));
                         }
                         return Future.succeededFuture();
                     })
                     .compose(v -> {
                         LiveMigrationStatus status = new LiveMigrationStatus(LiveMigrationStatus.MigrationState.COMPLETED,
                                                                              System.currentTimeMillis());
                         return statusTracker.setMigrationStatus(instanceMetadata, status)
                                             .compose(setStatus -> Future.succeededFuture(status));
                     })
                     .onSuccess(status -> {
                         LOGGER.info("Successfully updated Live Migration status as COMPLETED for instance: {}",
                                     instanceMetadata.host());
                         routingContext.response().end(Json.encode(status));
                     })
                     .onFailure(e -> {
                         LOGGER.error("Could not update Live Migration status as COMPLETED for instance {}",
                                      instanceMetadata.host(), e);
                         if (e instanceof IllegalStateException)
                         {
                             routingContext.fail(wrapHttpException(BAD_REQUEST,
                                                                   "Live migration marked as COMPLETED earlier."));
                         }
                         else
                         {
                             routingContext.fail(wrapHttpException(SERVICE_UNAVAILABLE,
                                                                   "Could not update Live Migration as complete."));
                         }
                     });
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.DATA_COPY.toAuthorization());
    }
}
