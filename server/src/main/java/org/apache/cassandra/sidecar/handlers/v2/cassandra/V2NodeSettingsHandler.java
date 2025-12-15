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
package org.apache.cassandra.sidecar.handlers.v2.cassandra;

import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions;
import org.apache.cassandra.sidecar.common.response.v2.V2NodeSettings;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.acl.authorization.DataResourceScope.DATA_SCOPE;

/**
 * V2NodeSettingsHandler is responsible for providing access to the configurations of the
 * Cassandra instances managed by this sidecar. This includes settings accessible via CQL from the system_views.settings table.
 */
public class V2NodeSettingsHandler extends AbstractHandler<Void> implements AccessProtected
{

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     */
    @Inject
    V2NodeSettingsHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext context, HttpServerRequest httpRequest, @NotNull String host, SocketAddress remoteAddress, Void request)
    {
        Map<String, String> cqlSettings = metadataFetcher.delegate(host).v2NodeSettings();
        V2NodeSettings v2nodeSettings = new V2NodeSettings(cqlSettings);
        context.json(v2nodeSettings);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
                Set<String> eligibleResources = Set.of(DATA_SCOPE.variableAwareResource(),
                                               // Keyspace access to system_views
                                               "data/system_views",
                                               // Access to all tables in keyspace system_views
                                               "data/system_views/*",
                                               // Access to the settings table in the system_views keyspace
                                               "data/system_views/settings");
        return Set.of(CassandraPermissions.SELECT.toAuthorization(eligibleResources));
    }
}
