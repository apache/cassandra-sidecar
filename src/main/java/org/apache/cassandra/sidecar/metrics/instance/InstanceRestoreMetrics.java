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

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.DeltaGauge;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link InstanceRestoreMetrics} contains metrics to track restore task for a Cassandra instance maintained by Sidecar.
 */
public class InstanceRestoreMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".Restore";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<Timer> sliceCompletionTime;
    public final NamedMetric<Timer> sliceImportTime;
    public final NamedMetric<Timer> sliceStageTime;
    public final NamedMetric<Timer> sliceUnzipTime;
    public final NamedMetric<Timer> sliceValidationTime;
    public final NamedMetric<DeltaGauge> sliceDownloadTimeouts;
    public final NamedMetric<DeltaGauge> sliceDownloadRetries;
    public final NamedMetric<DeltaGauge> sliceChecksumMismatches;
    public final NamedMetric<DefaultSettableGauge<Integer>> sliceImportQueueLength;
    public final NamedMetric<DefaultSettableGauge<Integer>> pendingSliceCount;
    public final NamedMetric<Histogram> dataSSTableComponentSize;
    public final NamedMetric<Timer> sliceDownloadTime;
    public final NamedMetric<Histogram> sliceCompressedSizeInBytes;
    public final NamedMetric<Histogram> sliceUncompressedSizeInBytes;
    public final NamedMetric<Timer> slowRestoreTaskTime;

    public InstanceRestoreMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        sliceCompletionTime
        = NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName("SliceCompletionTime").build();
        sliceImportTime
        = NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName("SliceImportTime").build();
        sliceStageTime
        = NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName("SliceStageTime").build();
        sliceUnzipTime
        = NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName("SliceUnzipTime").build();
        sliceValidationTime
        = NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName("SliceValidationTime").build();
        sliceDownloadTimeouts
        = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                     .withDomain(DOMAIN)
                     .withName("SliceDownloadTimeouts")
                     .build();
        sliceDownloadRetries
        = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                     .withDomain(DOMAIN)
                     .withName("SliceDownloadRetries")
                     .build();
        sliceChecksumMismatches
        = NamedMetric.builder(name -> metricRegistry.gauge(name, DeltaGauge::new))
                     .withDomain(DOMAIN)
                     .withName("SliceChecksumMismatches")
                     .build();
        sliceImportQueueLength
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("SliceImportQueueLength")
                     .build();
        pendingSliceCount
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("PendingSliceCount")
                     .build();
        dataSSTableComponentSize
        = NamedMetric.builder(metricRegistry::histogram)
                     .withDomain(DOMAIN)
                     .withName("RestoreDataSizeBytes")
                     .addTag("Component", "Data.db")
                     .build();
        sliceDownloadTime
        = NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName("SliceDownloadTime").build();
        sliceCompressedSizeInBytes
        = NamedMetric.builder(metricRegistry::histogram)
                     .withDomain(DOMAIN)
                     .withName("SliceCompressedSizeBytes")
                     .build();
        sliceUncompressedSizeInBytes
        = NamedMetric.builder(metricRegistry::histogram)
                     .withDomain(DOMAIN)
                     .withName("SliceUncompressedSizeBytes")
                     .build();
        slowRestoreTaskTime
        = NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName("SlowRestoreTaskTime").build();
    }
}
