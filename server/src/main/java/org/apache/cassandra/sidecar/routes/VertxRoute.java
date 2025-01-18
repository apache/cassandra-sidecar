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

import java.util.function.Consumer;

import io.vertx.ext.web.Router;

/**
 * Defines a vert.x route.
 */
public interface VertxRoute
{
    /**
     * Mount the route to router
     * @param router vertx router for routes definition
     */
    void mountTo(Router router);

    /**
     * A helper method to consume from {@link Router} and define route
     * @param routerConsumer router consumer
     * @return vertx route
     */
    static VertxRoute create(Consumer<Router> routerConsumer)
    {
        return routerConsumer::accept;
    }

    /**
     * An empty vertx route that does not mount anything to the router
     */
    VertxRoute EMPTY = create(r -> {});
}
