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

import com.codahale.metrics.MetricRegistry;

import static org.apache.cassandra.sidecar.metrics.ServerMetrics.SERVER_PREFIX;

/**
 * Tracks information about Sidecar coordination functionality
 */
public class CoordinationMetrics
{
    public static final String DOMAIN = SERVER_PREFIX + ".Coordination";
    /**
     * Keeps track of the lease-holder(s)
     */
    public final NamedMetric<DeltaGauge> leaseholders;
    /**
     * Keeps track of the number of instances participating in the selection of
     * the best-effort single instance executor
     */
    public final NamedMetric<DeltaGauge> participants;

    public CoordinationMetrics(MetricRegistry metricRegistry)
    {
        String domain = DOMAIN + ".ClusterLeaseClaim";
        leaseholders
        = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                     .withDomain(domain)
                     .withName("Leaseholder")
                     .build();
        participants
        = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                     .withDomain(domain)
                     .withName("Participant")
                     .build();
    }
}
