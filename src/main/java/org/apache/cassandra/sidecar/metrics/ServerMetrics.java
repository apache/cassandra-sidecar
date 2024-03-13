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

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Tracks metrics related to server initialization, periodic tasks related to server.
 */
@Singleton
public class ServerMetrics
{
    public static final String FEATURE = "sidecar.server";
    protected final MetricRegistry metricRegistry;
    protected final DefaultSettableGauge<Integer> cassandraInstancesUp;
    protected final DefaultSettableGauge<Integer> cassandraInstancesDown;

    @Inject
    public ServerMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "MetricRegistry can not be null");

        cassandraInstancesUp = metricRegistry.gauge(new MetricName(FEATURE, "instances_up").toString(),
                                                    () -> new DefaultSettableGauge<>(0));
        cassandraInstancesDown = metricRegistry.gauge(new MetricName(FEATURE, "instances_down").toString(),
                                                      () -> new DefaultSettableGauge<>(0));
    }

    public void recordInstancesUp(int count)
    {
        cassandraInstancesUp.setValue(count);
    }

    public void recordInstancesDown(int count)
    {
        cassandraInstancesDown.setValue(count);
    }
}
