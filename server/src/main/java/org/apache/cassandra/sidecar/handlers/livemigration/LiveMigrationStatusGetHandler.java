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

import com.google.inject.Inject;
import com.google.inject.Singleton;
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

/**
 * Handler to retrieve the current live migration status for an instance.
 * <p>
 * This endpoint allows clients to query the migration state of a specific instance,
 * returning a {@link org.apache.cassandra.sidecar.common.response.LiveMigrationStatus}
 * object containing the state (COMPLETED or NOT_COMPLETED) and timestamp.
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
        InstanceMetadata instance = metadataFetcher.instance(host);
        statusTracker.getMigrationStatus(instance)
                     .compose(routingContext::json)
                     .onFailure(e -> routingContext.response()
                                                   .setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code())
                                                   .end(e.getMessage()));
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.DATA_COPY.toAuthorization());
    }
}
