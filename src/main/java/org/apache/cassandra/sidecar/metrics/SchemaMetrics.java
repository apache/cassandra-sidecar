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

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Tracks metrics for {@link org.apache.cassandra.sidecar.db.schema.SidecarSchema} and other schema related handling
 * done by Sidecar
 */
@Singleton
public class SchemaMetrics
{
    public static final String DOMAIN = "Sidecar.Schema";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<DefaultSettableGauge<Integer>> failedInitializations;
    public final NamedMetric<DefaultSettableGauge<Integer>> failedModifications;

    @Inject
    public SchemaMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;

        failedInitializations
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("FailedInitializations")
                     .build();
        failedModifications
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("FailedModifications")
                     .build();
    }
}
