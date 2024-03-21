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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link UploadSSTableMetrics} tracks metrics captured during upload of SSTable components into Sidecar.
 */
public class UploadSSTableMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".upload_sstable";
    protected final MetricRegistry metricRegistry;
    protected final Map<String, UploadSSTableComponentMetrics> uploadComponentMetrics = new ConcurrentHashMap<>();

    public UploadSSTableMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;
    }

    public UploadSSTableComponentMetrics forComponent(String component)
    {
        return uploadComponentMetrics
               .computeIfAbsent(component, sstableComponent -> new UploadSSTableComponentMetrics(metricRegistry, sstableComponent));
    }

    /**
     * Metrics tracking during upload of a specific SSTable component
     */
    public static class UploadSSTableComponentMetrics
    {
        protected final MetricRegistry metricRegistry;
        public final String sstableComponent;
        public final NamedMetric<Meter> rateLimitedCalls;
        public final NamedMetric<Meter> diskUsageHighErrors;

        public UploadSSTableComponentMetrics(MetricRegistry metricRegistry, String sstableComponent)
        {
            this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");
            this.sstableComponent
            = Objects.requireNonNull(sstableComponent, "SSTable component required for component specific metrics capture");
            if (sstableComponent.isEmpty())
            {
                throw new IllegalArgumentException("SSTableComponent required for component specific metrics capture");
            }

            NamedMetric.Tag componentTag = NamedMetric.Tag.of("component", sstableComponent);

            rateLimitedCalls
            = NamedMetric.builder(metricRegistry::meter)
                         .withDomain(DOMAIN)
                         .withName("throttled_429")
                         .addTag(componentTag)
                         .build();
            diskUsageHighErrors
            = NamedMetric.builder(metricRegistry::meter)
                         .withDomain(DOMAIN)
                         .withName("disk_usage_high_errors")
                         .addTag(componentTag)
                         .build();
        }
    }

}
