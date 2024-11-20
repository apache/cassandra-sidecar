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

package org.apache.cassandra.sidecar.metrics;

import java.util.Objects;
import java.util.function.Function;

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static org.apache.cassandra.sidecar.metrics.ServerMetrics.SERVER_PREFIX;

/**
 * Tracks metrics related to restore functionality provided by Sidecar.
 */
public class RestoreMetrics
{
    public static final String DOMAIN = SERVER_PREFIX + ".Restore";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<Timer> sliceReplicationTime;
    public final NamedMetric<Timer> jobCompletionTime;
    public final NamedMetric<Timer> consistencyCheckTime;
    public final NamedMetric<DeltaGauge> successfulJobs;
    public final NamedMetric<DeltaGauge> failedJobs;
    public final NamedMetric<DefaultSettableGauge<Integer>> activeJobs;
    public final NamedMetric<DeltaGauge> tokenRefreshed;
    public final NamedMetric<DeltaGauge> tokenUnauthorized;
    public final NamedMetric<DeltaGauge> tokenExpired;

    public RestoreMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        sliceReplicationTime = createMetric("SliceReplicationTime", metricRegistry::timer);
        jobCompletionTime = createMetric("JobCompletionTime", metricRegistry::timer);
        consistencyCheckTime = createMetric("ConsistencyCheckTime", metricRegistry::timer);
        successfulJobs = createMetric("SuccessfulJobs", name -> metricRegistry.gauge(name, DeltaGauge::new));
        failedJobs = createMetric("FailedJobs", name -> metricRegistry.gauge(name, DeltaGauge::new));
        activeJobs = createMetric("ActiveJobs", name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)));
        tokenRefreshed = createMetric("TokenRefreshed", name -> metricRegistry.gauge(name, DeltaGauge::new));
        tokenUnauthorized = createMetric("TokenUnauthorized", name -> metricRegistry.gauge(name, DeltaGauge::new));
        tokenExpired = createMetric("TokenExpired",  name -> metricRegistry.gauge(name, DeltaGauge::new));
    }

    private <T extends Metric> NamedMetric<T> createMetric(String simpleName, Function<String, T> metricCreator)
    {
        return NamedMetric.builder(metricCreator)
                          .withDomain(DOMAIN)
                          .withName(simpleName)
                          .build();
    }
}
