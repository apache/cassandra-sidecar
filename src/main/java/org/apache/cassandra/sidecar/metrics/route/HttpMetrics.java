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

package org.apache.cassandra.sidecar.metrics.route;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.MetricName;

/**
 * {@link HttpMetrics} tracks metrics specific to a route provided by Sidecar. Metrics track values like requests
 * received by endpoint, response time taken by endpoint, etc.
 */
public class HttpMetrics
{
    public static final String FEATURE = "sidecar.http";
    protected final MetricRegistry metricRegistry;
    protected final String route;
    protected final Counter requests;
    protected final Timer responseTime;

    /**
     * Constructs an instance of HttpMetrics provided MetricRegistry for registering metrics and the route these metrics
     * are tracked for.
     * @param route route provided by Sidecar
     * @param metricRegistry dropwizard {@link MetricRegistry}
     */
    public HttpMetrics(String route, MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "MetricRegistry can not be null");
        this.route = Objects.requireNonNull(route, "Route required for tagging metrics related to route");

        requests
        = metricRegistry.counter(new MetricName(FEATURE, "requests", "route=" + route).toString());
        responseTime
        = metricRegistry.timer(new MetricName(FEATURE, "response_time", "route=" + route).toString());
    }

    public String route()
    {
        return route;
    }

    public void recordRequest()
    {
        requests.inc();
    }

    public void recordResponseTime(long duration, TimeUnit unit)
    {
        responseTime.update(duration, unit);
    }
}
