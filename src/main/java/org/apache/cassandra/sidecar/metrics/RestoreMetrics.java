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

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Tracks metrics related to restore functionality provided by Sidecar.
 */
@Singleton
public class RestoreMetrics
{
    public static final String DOMAIN = "sidecar.restore";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<Timer> sliceReplicationTime;
    public final NamedMetric<Timer> jobCompletionTime;
    public final NamedMetric<Meter> successfulJobs;
    public final NamedMetric<Meter> failedJobs;
    public final NamedMetric<DefaultSettableGauge<Integer>> activeJobs;
    public final NamedMetric<Meter> tokenRefreshes;
    public final NamedMetric<Meter> tokenUnauthorized;
    public final NamedMetric<Meter> tokenExpired;

    @Inject
    public RestoreMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        sliceReplicationTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("slice_replication_time")
                     .build();
        jobCompletionTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("job_completion_time")
                     .build();
        successfulJobs
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("successful_jobs")
                     .build();
        failedJobs
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("failed_jobs")
                     .build();
        activeJobs
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("active_jobs")
                     .build();
        tokenRefreshes
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("token_refreshes")
                     .build();
        tokenUnauthorized
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("token_unauthorized")
                     .build();
        tokenExpired
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("token_expired")
                     .build();
    }
}
