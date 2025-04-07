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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.DeltaGauge;
import org.apache.cassandra.sidecar.metrics.NamedMetric;
import org.jetbrains.annotations.NotNull;

/**
 * Tracks metrics for the schema reporting done by Sidecar
 */
public class SchemaReportingMetrics
{
    public static final String DOMAIN = ServerMetrics.SERVER_PREFIX + ".SchemaReporting";

    public final NamedMetric<DeltaGauge> startedRequest;
    public final NamedMetric<DeltaGauge> startedSchedule;
    public final NamedMetric<DeltaGauge> finishedSuccess;
    public final NamedMetric<DeltaGauge> finishedFailure;
    public final NamedMetric<Histogram> sizeAspects;
    public final NamedMetric<Timer> totalDuration;

    public SchemaReportingMetrics(@NotNull MetricRegistry registry)
    {
        startedSchedule = NamedMetric.builder(name -> registry.gauge(name, DeltaGauge::new))
                                     .withDomain(DOMAIN)
                                     .withName("Scheduled")
                                     .build();
        startedRequest = NamedMetric.builder(name -> registry.gauge(name, DeltaGauge::new))
                                    .withDomain(DOMAIN)
                                    .withName("Requested")
                                    .build();

        finishedSuccess = NamedMetric.builder(name -> registry.gauge(name, DeltaGauge::new))
                                     .withDomain(DOMAIN)
                                     .withName("Succeeded")
                                     .build();
        finishedFailure = NamedMetric.builder(name -> registry.gauge(name, DeltaGauge::new))
                                     .withDomain(DOMAIN)
                                     .withName("Failed")
                                     .build();

        sizeAspects = NamedMetric.builder(registry::histogram)
                                 .withDomain(DOMAIN)
                                 .withName("Aspects")
                                 .build();

        totalDuration = NamedMetric.builder(registry::timer)
                                   .withDomain(DOMAIN)
                                   .withName("Duration")
                                   .build();
    }
}
