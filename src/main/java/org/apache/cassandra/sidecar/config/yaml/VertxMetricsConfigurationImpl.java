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
import org.apache.cassandra.sidecar.config.VertxMetricsConfiguration;

/**
 * Configuration needed for capturing metrics release by Vert.x framework.
 */
public class VertxMetricsConfigurationImpl implements VertxMetricsConfiguration
{
    public static final boolean DEFAULT_ENABLED = true;
    public static final String DEFAULT_DROPWIZARD_REGISTRY_NAME = "cassandra_sidecar";
    public static final boolean DEFAULT_EXPOSE_VIA_JMX = false;
    public static final String DEFAULT_JMX_DOMAIN_NAME = "sidecar.vertx.jmx_domain";
    public static final List<String> DEFAULT_MONITORED_SERVER_ROUTE_REGEXES = Collections.singletonList("/api/v1/*");

    @JsonProperty(value = "enabled")
    protected final boolean enabled;
    @JsonProperty(value = "registry_name")
    protected final String registryName;
    @JsonProperty(value = "expose_via_jmx")
    protected final boolean exposeViaJMX;
    @JsonProperty(value = "jmx_domain_name")
    protected final String jmxDomainName;
    @JsonProperty(value = "monitored_server_route_regexes")
    protected final List<String> monitoredServerRouteRegexes;

    public VertxMetricsConfigurationImpl()
    {
        this(DEFAULT_ENABLED,
             DEFAULT_DROPWIZARD_REGISTRY_NAME,
             DEFAULT_EXPOSE_VIA_JMX,
             DEFAULT_JMX_DOMAIN_NAME,
             DEFAULT_MONITORED_SERVER_ROUTE_REGEXES);
    }

    public VertxMetricsConfigurationImpl(boolean enabled,
                                         String registryName,
                                         boolean exposeViaJMX,
                                         String jmxDomainName,
                                         List<String> monitoredServerRouteRegexes)
    {
        this.enabled = enabled;
        this.registryName = registryName;
        this.exposeViaJMX = exposeViaJMX;
        this.jmxDomainName = jmxDomainName;
        this.monitoredServerRouteRegexes = monitoredServerRouteRegexes;
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
    public boolean exposeViaJMX()
    {
        return exposeViaJMX;
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
    public List<String> monitoredServerRouteRegexes()
    {
        return monitoredServerRouteRegexes;
    }
}
