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

package org.apache.cassandra.sidecar.metrics.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.server.ServerMetrics.SERVER_PREFIX;

/**
 * Track metrics related to the authentication and authorization subsystems in Sidecar.
 */
public class AuthMetrics
{
    private static final String DOMAIN = SERVER_PREFIX + ".Auth";
    public final NamedMetric<Counter> jwtPemRefreshFailures;
    public final NamedMetric<Counter> jwtPemRefreshSuccesses;
    /**
     * Captures the time to successfully authorize non-cached requests. Cached authorization requests will
     * not be recorded in this metric
     */
    public final NamedMetric<Timer> authorizationTime;

    public AuthMetrics(MetricRegistry metricRegistry)
    {
        jwtPemRefreshFailures = NamedMetric.builder(metricRegistry::counter)
                                        .withDomain(DOMAIN)
                                        .withName("JwtPemRefreshFailures")
                                        .build();

        jwtPemRefreshSuccesses = NamedMetric.builder(metricRegistry::counter)
                                         .withDomain(DOMAIN)
                                         .withName("JwtPemRefreshSuccesses")
                                         .build();

        authorizationTime = NamedMetric.builder(metricRegistry::timer)
                                       .withDomain(DOMAIN)
                                       .withName("authorizationTime")
                                       .build();
    }
}
