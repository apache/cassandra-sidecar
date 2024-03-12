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

/**
 * Tracks metrics related to a Cassandra instance that Sidecar maintains.
 */
public class InstanceMetricsImpl implements InstanceMetrics
{
    protected final InstanceMetricRegistry instanceMetricRegistry;
    protected final Map<String, StreamSSTableComponentMetrics> streamComponentMetrics = new ConcurrentHashMap<>();
    protected final Map<String, UploadSSTableComponentMetrics> uploadComponentMetrics = new ConcurrentHashMap<>();
    protected final InstanceResourceMetrics instanceResourceMetrics;

    public InstanceMetricsImpl(InstanceMetricRegistry instanceMetricRegistry)
    {
        this.instanceMetricRegistry
        = Objects.requireNonNull(instanceMetricRegistry, "Metrics registry can not be null");

        this.instanceResourceMetrics = new InstanceResourceMetrics(instanceMetricRegistry);
    }

    @Override
    public InstanceResourceMetrics resource()
    {
        return instanceResourceMetrics;
    }

    @Override
    public StreamSSTableComponentMetrics forStreamComponent(String component)
    {
        return streamComponentMetrics
               .computeIfAbsent(component, sstableComponent -> new StreamSSTableComponentMetrics(instanceMetricRegistry,
                                                                                                 sstableComponent));
    }

    @Override
    public UploadSSTableComponentMetrics forUploadComponent(String component)
    {
        return uploadComponentMetrics
               .computeIfAbsent(component, sstableComponent -> new UploadSSTableComponentMetrics(instanceMetricRegistry,
                                                                                                 sstableComponent));
    }
}
