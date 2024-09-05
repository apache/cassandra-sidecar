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

import com.codahale.metrics.MetricRegistry;

/**
 * Tracks metrics related to a Cassandra instance that Sidecar maintains.
 */
public class InstanceMetricsImpl implements InstanceMetrics
{
    protected final MetricRegistry metricRegistry;
    protected final StreamSSTableMetrics streamSSTableMetrics;
    protected final UploadSSTableMetrics uploadSSTableMetrics;
    protected final InstanceHealthMetrics healthMetrics;
    protected final InstanceResourceMetrics resourceMetrics;
    protected final InstanceRestoreMetrics restoreMetrics;
    protected final SSTableImportMetrics ssTableImportMetrics;

    public InstanceMetricsImpl(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metrics registry can not be null");

        this.healthMetrics = new InstanceHealthMetrics(metricRegistry);
        this.resourceMetrics = new InstanceResourceMetrics(metricRegistry);
        this.streamSSTableMetrics = new StreamSSTableMetrics(metricRegistry);
        this.uploadSSTableMetrics = new UploadSSTableMetrics(metricRegistry);
        this.restoreMetrics = new InstanceRestoreMetrics(metricRegistry);
        this.ssTableImportMetrics = new SSTableImportMetrics(metricRegistry);
    }

    @Override
    public InstanceHealthMetrics health()
    {
        return healthMetrics;
    }

    @Override
    public InstanceResourceMetrics resource()
    {
        return resourceMetrics;
    }

    @Override
    public StreamSSTableMetrics streamSSTable()
    {
        return streamSSTableMetrics;
    }

    @Override
    public UploadSSTableMetrics uploadSSTable()
    {
        return uploadSSTableMetrics;
    }

    @Override
    public InstanceRestoreMetrics restore()
    {
        return restoreMetrics;
    }

    @Override
    public SSTableImportMetrics sstableImport()
    {
        return ssTableImportMetrics;
    }
}
