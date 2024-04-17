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

package org.apache.cassandra.sidecar.metrics;

import java.util.Objects;

import com.codahale.metrics.MetricRegistry;

/**
 * {@link ServerMetrics} tracks metrics related to Sidecar server.
 */
public class ServerMetricsImpl implements ServerMetrics
{
    protected final MetricRegistry metricRegistry;
    protected final HealthMetrics healthMetrics;
    protected final ResourceMetrics resourceMetrics;
    protected final RestoreMetrics restoreMetrics;
    protected final SchemaMetrics schemaMetrics;

    public ServerMetricsImpl(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "MetricRegistry can not be null");

        this.healthMetrics = new HealthMetrics(metricRegistry);
        this.resourceMetrics = new ResourceMetrics(metricRegistry);
        this.restoreMetrics = new RestoreMetrics(metricRegistry);
        this.schemaMetrics = new SchemaMetrics(metricRegistry);
    }

    @Override
    public HealthMetrics health()
    {
        return healthMetrics;
    }

    @Override
    public ResourceMetrics resource()
    {
        return resourceMetrics;
    }

    @Override
    public RestoreMetrics restore()
    {
        return restoreMetrics;
    }

    @Override
    public SchemaMetrics schema()
    {
        return schemaMetrics;
    }
}
