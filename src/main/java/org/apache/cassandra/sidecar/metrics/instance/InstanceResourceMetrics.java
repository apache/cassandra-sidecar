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
import org.apache.cassandra.sidecar.metrics.MetricName;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link InstanceResourceMetrics} contains metrics to track resource usage of a Cassandra instance maintained
 * by Sidecar.
 */
public class InstanceResourceMetrics
{
    public static final String FEATURE = INSTANCE_PREFIX + ".resource";
    protected final InstanceMetricRegistry metricRegistry;
    protected final Meter insufficientStorageErrors;

    public InstanceResourceMetrics(InstanceMetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        insufficientStorageErrors
        = metricRegistry.meter(new MetricName(FEATURE, "insufficient_storage_errors").toString());
    }

    public void recordInsufficientStorageError()
    {
        insufficientStorageErrors.mark();
    }
}
