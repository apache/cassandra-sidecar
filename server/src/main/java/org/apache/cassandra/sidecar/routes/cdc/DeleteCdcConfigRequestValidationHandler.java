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
import java.util.Set;
import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.routes.AccessProtected;


/**
 * Deleting a service config from "configs" table in sidecar keyspace should perform some
 * validation checks before deleting the config. {@link DeleteCdcConfigRequestValidationHandler}
 * performs those validations before the delete operation.
 */
@Singleton
public class DeleteCdcConfigRequestValidationHandler implements Handler<RoutingContext> , AccessProtected
{
    @Override
    public void handle(RoutingContext context)
    {
        JsonObject payload = context.body().asJsonObject();
        ServiceConfigValidator.validateService(context, payload);
        context.next();
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.CDC.toAuthorization());
    }
}
