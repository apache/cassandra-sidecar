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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link InstanceRestoreMetrics} contains metrics to track restore task for a Cassandra instance maintained by Sidecar.
 */
public class InstanceRestoreMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".restore";
    protected final MetricRegistry metricRegistry;
    public final NamedMetric<Timer> sliceCompletionTime;
    public final NamedMetric<Timer> sliceImportTime;
    public final NamedMetric<Timer> sliceStageTime;
    public final NamedMetric<Timer> sliceUnzipTime;
    public final NamedMetric<Timer> sliceValidationTime;
    public final NamedMetric<Meter> sliceDownloadTimeouts;
    public final NamedMetric<Meter> sliceDownloadRetries;
    public final NamedMetric<Meter> sliceChecksumMismatches;
    public final NamedMetric<DefaultSettableGauge<Integer>> sliceImportQueueLength;
    public final NamedMetric<DefaultSettableGauge<Integer>> pendingSliceCount;
    public final NamedMetric<DefaultSettableGauge<Long>> dataSSTableComponentSize;
    public final NamedMetric<Timer> sliceDownloadTime;
    public final NamedMetric<DefaultSettableGauge<Long>> sliceCompressedSizeInBytes;
    public final NamedMetric<DefaultSettableGauge<Long>> sliceUncompressedSizeInBytes;
    public final NamedMetric<Timer> longRunningRestoreHandler;

    public InstanceRestoreMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        sliceCompletionTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("slice_completion_time")
                     .build();
        sliceImportTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("slice_import_time")
                     .build();
        sliceStageTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("slice_stage_time")
                     .build();
        sliceUnzipTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("slice_unzip_time")
                     .build();
        sliceValidationTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("slice_validation_time")
                     .build();
        sliceDownloadTimeouts
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("slice_download_timeouts")
                     .build();
        sliceDownloadRetries
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("slice_completion_retries")
                     .build();
        sliceChecksumMismatches
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("slice_checksum_mismatches")
                     .build();
        sliceImportQueueLength
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("slice_import_queue_length")
                     .build();
        pendingSliceCount
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("pending_slice_count")
                     .build();
        dataSSTableComponentSize
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0L)))
                     .withDomain(DOMAIN)
                     .withName("restore_size")
                     .addTag("component", "db")
                     .build();
        sliceDownloadTime
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("slice_download_time")
                     .build();
        sliceCompressedSizeInBytes
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0L)))
                     .withDomain(DOMAIN)
                     .withName("slice_compressed_size_bytes")
                     .build();
        sliceUncompressedSizeInBytes
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0L)))
                     .withDomain(DOMAIN)
                     .withName("slice_uncompressed_size_bytes")
                     .build();
        longRunningRestoreHandler
        = NamedMetric.builder(metricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("long_running_restore_handler")
                     .build();
    }
}
