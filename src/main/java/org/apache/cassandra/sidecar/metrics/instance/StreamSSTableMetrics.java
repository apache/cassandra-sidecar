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
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link StreamSSTableMetrics} tracks metrics captured during streaming of SSTable components from Sidecar.
 */
public class StreamSSTableMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".stream_sstable";
    protected final MetricRegistry metricRegistry;
    protected final Map<String, StreamSSTableComponentMetrics> streamComponentMetrics = new ConcurrentHashMap<>();

    public StreamSSTableMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;
    }

    public StreamSSTableComponentMetrics forComponent(String component)
    {
        return streamComponentMetrics
               .computeIfAbsent(component, sstableComponent -> new StreamSSTableComponentMetrics(metricRegistry,
                                                                                                 sstableComponent));
    }

    /**
     * Metrics tracked during streaming of a specific SSTable component
     */
    public static class StreamSSTableComponentMetrics
    {
        protected final MetricRegistry metricRegistry;
        public final String sstableComponent;
        public final NamedMetric<Meter> rateLimitedCalls;
        public final NamedMetric<Timer> sendFileLatency;

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

            rateLimitedCalls
            = NamedMetric.builder(metricRegistry::meter)
                         .withDomain(DOMAIN)
                         .withName("throttled_429")
                         .addTag(componentTag)
                         .build();
            sendFileLatency
            = NamedMetric.builder(metricRegistry::timer)
                         .withDomain(DOMAIN)
                         .withName("sendfile_latency")
                         .addTag(componentTag)
                         .build();
        }
    }
}
