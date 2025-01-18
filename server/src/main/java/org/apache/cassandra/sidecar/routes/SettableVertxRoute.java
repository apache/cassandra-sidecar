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

package org.apache.cassandra.sidecar.routes;

import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.sidecar.modules.multibindings.RouteClassKey;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A {@link VertxRoute} that allows to set the fields using RouteClassKey class
 */
public abstract class SettableVertxRoute implements VertxRoute
{
    private HttpMethod httpMethod;
    private String routeURI;

    public void setRouteClassKey(Class<? extends RouteClassKey> routeClassKey)
    {
        this.httpMethod = RouteClassKey.httpMethod(routeClassKey);
        this.routeURI = RouteClassKey.routeURI(routeClassKey);
    }

    public HttpMethod httpMethod()
    {
        return httpMethod;
    }

    public String routeURI()
    {
        return routeURI;
    }

    @VisibleForTesting
    public void setHttpMethod(HttpMethod httpMethod)
    {
        this.httpMethod = httpMethod;
    }

    @VisibleForTesting
    public void setRouteURI(String routeURI)
    {
        this.routeURI = routeURI;
    }
}
