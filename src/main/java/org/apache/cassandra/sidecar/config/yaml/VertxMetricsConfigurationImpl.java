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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.VertxMetricsConfiguration;

/**
 * Configuration needed for capturing metrics release by Vert.x framework.
 */
public class VertxMetricsConfigurationImpl implements VertxMetricsConfiguration
{
    public static final boolean DEFAULT_ENABLED = true;
    public static final String DEFAULT_DROPWIZARD_REGISTRY_NAME = "cassandra_sidecar";
    public static final boolean DEFAULT_JMX_ENABLED = true;
    public static final String DEFAULT_JMX_DOMAIN_NAME = "sidecar.vertx.jmx_domain";
    public static final String DEFAULT_MONITORED_SERVER_ROUTES_REGEX = "/api/v1/*";

    @JsonProperty(value = "enabled")
    protected final boolean enabled;
    @JsonProperty(value = "registry_name")
    protected final String registryName;
    @JsonProperty(value = "jmx_enabled")
    protected final boolean jmxEnabled;
    @JsonProperty(value = "jmx_domain_name")
    protected final String jmxDomainName;
    @JsonProperty(value = "monitored_server_routes_regex")
    protected final String monitoredServerRoutesRegex;

    public VertxMetricsConfigurationImpl()
    {
        this(DEFAULT_ENABLED,
             DEFAULT_DROPWIZARD_REGISTRY_NAME,
             DEFAULT_JMX_ENABLED,
             DEFAULT_JMX_DOMAIN_NAME,
             DEFAULT_MONITORED_SERVER_ROUTES_REGEX);
    }

    public VertxMetricsConfigurationImpl(boolean enabled,
                                         String registryName,
                                         boolean jmxEnabled,
                                         String jmxDomainName,
                                         String monitoredServerRoutesRegex)
    {
        this.enabled = enabled;
        this.registryName = registryName;
        this.jmxEnabled = jmxEnabled;
        this.jmxDomainName = jmxDomainName;
        this.monitoredServerRoutesRegex = monitoredServerRoutesRegex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean enabled()
    {
        return enabled;
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
    public boolean jmxEnabled()
    {
        return jmxEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String jmxDomainName()
    {
        return jmxDomainName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String monitoredServerRoutesRegex()
    {
        return monitoredServerRoutesRegex;
    }
}
