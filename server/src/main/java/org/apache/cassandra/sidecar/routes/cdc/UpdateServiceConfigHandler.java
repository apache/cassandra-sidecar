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
package org.apache.cassandra.sidecar.routes.cdc;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.db.ConfigAccessor;
import org.apache.cassandra.sidecar.db.ConfigAccessorFactory;
import org.apache.cassandra.sidecar.routes.AccessProtected;

/**
 * Provides REST endpoint for updating CDC/Kafka configs.
 */
@Singleton
public class UpdateServiceConfigHandler implements Handler<RoutingContext>, AccessProtected
{
    private final ConfigAccessorFactory configAccessorFactory;

    @Inject
    public UpdateServiceConfigHandler(final ConfigAccessorFactory configAccessorFactory)
    {
        this.configAccessorFactory = configAccessorFactory;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.CDC.toAuthorization());
    }

    @Override
    public void handle(RoutingContext context)
    {
        JsonObject body = context.getBodyAsJson();
        final String service = context.pathParam(ConfigPayloadParams.SERVICE);
        ConfigAccessor accessor = configAccessorFactory.getConfigAccessor(service);
        Map<String, String> config = context.getBodyAsJson().getJsonObject(ConfigPayloadParams.CONFIG).getMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
        accessor.storeConfig(config);
        context.json(body);
    }
}
