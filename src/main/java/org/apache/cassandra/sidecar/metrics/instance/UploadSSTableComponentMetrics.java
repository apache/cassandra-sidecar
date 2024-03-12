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
 * {@link UploadSSTableComponentMetrics} tracks metrics captured during upload of SSTable component into Sidecar.
 */
public class UploadSSTableComponentMetrics
{
    public static final String FEATURE = INSTANCE_PREFIX + ".upload";
    protected final InstanceMetricRegistry instanceMetricRegistry;
    protected final String sstableComponent;
    protected final Meter rateLimitedCalls;
    protected final Meter diskUsageHighErrors;

    public UploadSSTableComponentMetrics(InstanceMetricRegistry instanceMetricRegistry,
                                         String sstableComponent)
    {
        this.instanceMetricRegistry
        = Objects.requireNonNull(instanceMetricRegistry, "Metric registry can not be null");
        this.sstableComponent
        = Objects.requireNonNull(sstableComponent, "SSTable component required for component specific metrics capture");
        if (sstableComponent.isEmpty())
        {
            throw new IllegalArgumentException("SSTableComponent required for component specific metrics capture");
        }

        rateLimitedCalls
        = instanceMetricRegistry.meter(new MetricName(FEATURE,
                                                      "rate_limited_calls_429",
                                                      new MetricName.Tag("component", sstableComponent)).toString());
        diskUsageHighErrors
        = instanceMetricRegistry.meter(new MetricName(FEATURE,
                                                      "disk_usage_high_errors",
                                                      new MetricName.Tag("component", sstableComponent)).toString());
    }

    public String sstableComponent()
    {
        return sstableComponent;
    }

    public void recordRateLimitedCall()
    {
        rateLimitedCalls.mark();
    }

    public void recordDiskUsageHighError()
    {
        diskUsageHighErrors.mark();
    }
}
