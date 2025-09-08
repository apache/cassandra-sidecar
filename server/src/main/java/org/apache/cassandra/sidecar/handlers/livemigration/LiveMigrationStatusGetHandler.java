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

import java.util.Objects;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationStatusTracker;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

/**
 * Handler to retrieve the current live migration status for an instance.
 * Returns COMPLETED if the migration has finished, or NOT_COMPLETED if it's still in progress or not started.
 */
@Singleton
public class LiveMigrationStatusGetHandler extends AbstractHandler<Void> implements AccessProtected
{

    private final LiveMigrationStatusTracker statusTracker;

    @Inject
    public LiveMigrationStatusGetHandler(InstanceMetadataFetcher metadataFetcher,
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
        executorPools.service().runBlocking(() -> {
            InstanceMetadata instance = metadataFetcher.instance(host);
            LiveMigrationStatus status =
            Objects.requireNonNullElseGet(statusTracker.getMigrationStatus(instance),
                                          () -> new LiveMigrationStatus(MigrationState.NOT_COMPLETED, null));
            routingContext.response()
                          .send(Json.encode(status));
        });
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.STREAM_FILES.toAuthorization());
    }
}
