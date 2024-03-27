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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.MetricsConfiguration;
import org.apache.cassandra.sidecar.config.MetricsFilteringConfiguration;
import org.apache.cassandra.sidecar.config.VertxMetricsConfiguration;

/**
 * Configuration needed for capturing metrics.
 */
public class MetricsConfigurationImpl implements MetricsConfiguration
{
    public static final String DEFAULT_DROPWIZARD_REGISTRY_NAME = "cassandra_sidecar";
    public static final VertxMetricsConfiguration DEFAULT_VERTX_METRICS_CONFIGURATION
    = new VertxMetricsConfigurationImpl();

    @JsonProperty(value = "registry_name")
    protected final String registryName;
    @JsonProperty(value = "vertx")
    protected final VertxMetricsConfiguration vertxConfiguration;
    @JsonProperty(value = "filtering")
    protected final List<MetricsFilteringConfiguration> filteringConfigurations;

    public MetricsConfigurationImpl()
    {
        this(DEFAULT_DROPWIZARD_REGISTRY_NAME, DEFAULT_VERTX_METRICS_CONFIGURATION, Collections.emptyList());
    }

    public MetricsConfigurationImpl(String registryName,
                                    VertxMetricsConfiguration vertxConfiguration,
                                    List<MetricsFilteringConfiguration> filteringConfigurations)
    {
        this.registryName = registryName;
        this.vertxConfiguration = vertxConfiguration;
        this.filteringConfigurations = filteringConfigurations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String registryName()
    {
        return registryName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VertxMetricsConfiguration vertxConfiguration()
    {
        return vertxConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<MetricsFilteringConfiguration> filteringConfigurations()
    {
        return filteringConfigurations;
    }
}
