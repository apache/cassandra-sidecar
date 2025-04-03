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

import static org.apache.cassandra.sidecar.metrics.ServerMetrics.SERVER_PREFIX;

/**
 * Tracks metrics related to cdc functionality provided by Sidecar.
 */
public class CdcMetrics
{
    public static final String DOMAIN = SERVER_PREFIX + ".Cdc";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<DeltaGauge> cdcRawCleanerFailed;
    public final NamedMetric<DeltaGauge> orphanedIdx;
    public final NamedMetric<DefaultSettableGauge<Integer>> oldestSegmentAge;
    public final NamedMetric<DeltaGauge> totalConsumedCdcBytes;
    public final NamedMetric<DefaultSettableGauge<Long>> totalCdcSpaceUsed;
    public final NamedMetric<DeltaGauge> deletedSegment;
    public final NamedMetric<DeltaGauge> lowCdcRawSpace;
    public final NamedMetric<DeltaGauge> criticalCdcRawSpace;

    public CdcMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");
        this.cdcRawCleanerFailed = createMetric("CleanerFailed", name -> metricRegistry.gauge(name, DeltaGauge::new));
        this.totalConsumedCdcBytes = createMetric("TotalConsumedBytes", name -> metricRegistry.gauge(name, DeltaGauge::new));
        this.totalCdcSpaceUsed = createMetric("TotalSpaceUsed", name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0L)));
        this.orphanedIdx = createMetric("OrphanedIdxFile", name -> metricRegistry.gauge(name, DeltaGauge::new));
        this.deletedSegment = createMetric("DeletedSegment", name -> metricRegistry.gauge(name, DeltaGauge::new));
        this.oldestSegmentAge = createMetric("OldestSegmentAgeSeconds", name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)));
        this.lowCdcRawSpace = createMetric("LowSpace", name -> metricRegistry.gauge(name, DeltaGauge::new));
        this.criticalCdcRawSpace = createMetric("CriticalSpace", name -> metricRegistry.gauge(name, DeltaGauge::new));
    }

    private <T extends Metric> NamedMetric<T> createMetric(String simpleName, Function<String, T> metricCreator)
    {
        return NamedMetric.builder(metricCreator)
                          .withDomain(DOMAIN)
                          .withName(simpleName)
                          .build();
    }
}
