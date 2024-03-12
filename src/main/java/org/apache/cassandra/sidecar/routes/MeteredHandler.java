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

import java.util.concurrent.TimeUnit;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.metrics.route.HttpMetrics;

/**
 * Handler for tracking metrics for an endpoint it is created for.
 */
public class MeteredHandler implements Handler<RoutingContext>
{
    private final HttpMetrics metrics;

    public MeteredHandler(HttpMetrics httpMetrics)
    {
        this.metrics = httpMetrics;
    }

    @Override
    public void handle(RoutingContext context)
    {
        metrics.recordRequest();

        long startNanoTime = System.nanoTime();
        context.addEndHandler(v -> {

            long endNanoTime = System.nanoTime();
            metrics.recordResponseTime(endNanoTime - startNanoTime, TimeUnit.NANOSECONDS);
        });
        context.next();
    }
}
