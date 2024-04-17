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
import com.codahale.metrics.MetricRegistry;

import static org.apache.cassandra.sidecar.metrics.ServerMetrics.SERVER_PREFIX;

/**
 * Tracks availability of Cassandra instances maintained by Sidecar.
 */
public class HealthMetrics
{
    public static final String DOMAIN = SERVER_PREFIX + ".Health";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<DefaultSettableGauge<Integer>> cassandraInstancesUp;
    public final NamedMetric<DefaultSettableGauge<Integer>> cassandraInstancesDown;

    public HealthMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        cassandraInstancesUp =
        NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                   .withDomain(DOMAIN)
                   .withName("CassInstancesUp")
                   .build();
        cassandraInstancesDown =
        NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                   .withDomain(DOMAIN)
                   .withName("CassInstancesDown")
                   .build();
    }
}
