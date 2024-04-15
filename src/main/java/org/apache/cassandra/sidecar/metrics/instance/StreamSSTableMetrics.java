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

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link StreamSSTableMetrics} tracks metrics captured during streaming of SSTable components from Sidecar.
 */
public class StreamSSTableMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".StreamSSTable";
    protected final MetricRegistry metricRegistry;
    protected final Map<String, StreamSSTableComponentMetrics> streamComponentMetrics = new ConcurrentHashMap<>();
    public final NamedMetric<Meter> totalBytesStreamedRate;
    public final NamedMetric<DefaultSettableGauge<Integer>> rateLimitedCalls;

    public StreamSSTableMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        totalBytesStreamedRate
        = NamedMetric.builder(metricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("TotalBytesStreamedRate")
                     .build();
        rateLimitedCalls
        = NamedMetric.builder(name -> metricRegistry.gauge(name, () -> new DefaultSettableGauge<>(0)))
                     .withDomain(DOMAIN)
                     .withName("Throttled")
                     .build();
    }

    public StreamSSTableComponentMetrics forComponent(String component)
    {
        return streamComponentMetrics
               .computeIfAbsent(component, sstableComponent -> new StreamSSTableComponentMetrics(metricRegistry, sstableComponent));
    }

    /**
     * Metrics tracked during streaming of a specific SSTable component
     */
    public static class StreamSSTableComponentMetrics
    {
        protected final MetricRegistry metricRegistry;
        public final String sstableComponent;
        public final NamedMetric<Meter> bytesStreamedRate;

        public StreamSSTableComponentMetrics(MetricRegistry metricRegistry, String sstableComponent)
        {
            this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");
            this.sstableComponent
            = Objects.requireNonNull(sstableComponent, "SSTable component required for component specific metrics capture");
            if (sstableComponent.isEmpty())
            {
                throw new IllegalArgumentException("SSTableComponent required for component specific metrics capture");
            }

            NamedMetric.Tag componentTag = NamedMetric.Tag.of("component", sstableComponent);

            bytesStreamedRate
            = NamedMetric.builder(metricRegistry::meter)
                         .withDomain(DOMAIN)
                         .withName("BytesStreamedRate")
                         .addTag(componentTag)
                         .build();
        }
    }
}
