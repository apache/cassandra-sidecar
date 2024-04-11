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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Tracks resource metrics, for resources maintained by Sidecar.
 */
@Singleton
public class ResourceMetrics
{
    public static final String DOMAIN = "Sidecar.Resource";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<Timer> serviceTaskTime;
    public final NamedMetric<Timer> internalTaskTime;

    @Inject
    public ResourceMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        serviceTaskTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("ShortTaskTime")
                     .build();
        internalTaskTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("LongTaskTime")
                     .build();
    }
}
