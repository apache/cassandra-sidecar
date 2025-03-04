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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.request.Service;
import org.apache.cassandra.sidecar.common.request.data.AllServicesConfigPayload;
import org.apache.cassandra.sidecar.db.ConfigAccessor;
import org.apache.cassandra.sidecar.db.ConfigAccessorFactory;
import org.apache.cassandra.sidecar.routes.AccessProtected;

/**
 * Provides REST endpoint for getting all the configs in "configs" table in sidecar internal keyspace.
 */
@Singleton
public class AllServiceConfigHandler implements Handler<RoutingContext>, AccessProtected
{
    private final ConfigAccessorFactory configAccessorFactory;

    @Inject
    public AllServiceConfigHandler(final ConfigAccessorFactory configAccessorFactory)
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
        List<AllServicesConfigPayload.Service> services = new ArrayList<>();
        for (Service service : Service.values())
        {
            ConfigAccessor accessor = configAccessorFactory.configAccessor(service);
            Map<String, String> config = accessor.getConfig().getConfigs();

            AllServicesConfigPayload.Service serviceConfig = new AllServicesConfigPayload.Service(service.serviceName, config);

            services.add(serviceConfig);
        }
        AllServicesConfigPayload payload = new AllServicesConfigPayload(services);
        context.json(payload);
    }
}
