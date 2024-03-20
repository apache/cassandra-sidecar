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
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.NamedMetric;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link StreamSSTableComponentMetrics} tracks metrics captured during streaming of SSTable component from Sidecar.
 */
public class StreamSSTableComponentMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".stream";
    protected final InstanceMetricRegistry instanceMetricRegistry;
    public final String sstableComponent;
    public final Meter rateLimitedCalls;
    public final Timer timeTakenForSendFile;
    public final Timer waitTimeSent;

    public StreamSSTableComponentMetrics(InstanceMetricRegistry instanceMetricRegistry,
                                         String sstableComponent)
    {
        this.instanceMetricRegistry = Objects.requireNonNull(instanceMetricRegistry, "Metric registry can not be null");
        this.sstableComponent
        = Objects.requireNonNull(sstableComponent, "SSTable component required for component specific metrics capture");
        if (sstableComponent.isEmpty())
        {
            throw new IllegalArgumentException("SSTableComponent required for component specific metrics capture");
        }

        NamedMetric.Tag componentTag = NamedMetric.Tag.of("component", sstableComponent);

        rateLimitedCalls
        = NamedMetric.builder(instanceMetricRegistry::meter)
                     .withDomain(DOMAIN)
                     .withName("rate_limited_calls_429")
                     .addTag(componentTag)
                     .build().metric;
        timeTakenForSendFile
        = NamedMetric.builder(instanceMetricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("time_taken_for_sendfile")
                     .addTag(componentTag)
                     .build().metric;
        waitTimeSent
        = NamedMetric.builder(instanceMetricRegistry::timer)
                     .withDomain(DOMAIN)
                     .withName("429_wait_time")
                     .addTag(componentTag)
                     .build().metric;
    }
}
