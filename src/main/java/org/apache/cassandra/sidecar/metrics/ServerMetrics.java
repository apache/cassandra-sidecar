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
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Tracks metrics related to server initialization, periodic tasks related to server.
 */
@Singleton
public class ServerMetrics
{
    public static final String DOMAIN = "Sidecar.Server";
    protected final MetricRegistry metricRegistry;
    // TODO - Saranya, should those be in HealthMetrics?
    public final NamedMetric<DefaultSettableGauge<Integer>> cassandraInstancesUp;
    public final NamedMetric<DefaultSettableGauge<Integer>> cassandraInstancesDown;

    public final RestoreMetrics restoreMetrics;
    public final SchemaMetrics schemaMetrics;

    @Inject
    public ServerMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "MetricRegistry can not be null");

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

        restoreMetrics = new RestoreMetrics(metricRegistry);
        schemaMetrics = new SchemaMetrics(metricRegistry);
        // TODO - Saranya, group other server level metrics under ServerMetrics; the individual metrics does not need to be declared as @Singleton
    }
}
