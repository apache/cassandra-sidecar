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

package org.apache.cassandra.sidecar.metrics.instance;

import java.util.Objects;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * Tracks health related metrics of a Cassandra instance maintained by Sidecar.
 */
public class InstanceHealthMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".health";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<Meter> jmxDown;
    public final NamedMetric<Meter> nativeDown;

    public InstanceHealthMetrics(MetricRegistry registry)
    {
        this.metricRegistry = Objects.requireNonNull(registry, "Metric registry can not be null");

        jmxDown
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("jmx_down")
                     .build();
        nativeDown
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("native_down")
                     .build();
    }
}
