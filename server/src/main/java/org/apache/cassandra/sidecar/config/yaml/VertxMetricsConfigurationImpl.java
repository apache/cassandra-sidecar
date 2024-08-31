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
    public static final boolean DEFAULT_EXPOSE_VIA_JMX = false;
    public static final String DEFAULT_JMX_DOMAIN_NAME = "sidecar.vertx.jmx_domain";

    @JsonProperty(value = "enabled")
    protected final boolean enabled;
    @JsonProperty(value = "expose_via_jmx")
    protected final boolean exposeViaJMX;
    @JsonProperty(value = "jmx_domain_name")
    protected final String jmxDomainName;

    public VertxMetricsConfigurationImpl()
    {
        this(DEFAULT_ENABLED,
             DEFAULT_EXPOSE_VIA_JMX,
             DEFAULT_JMX_DOMAIN_NAME);
    }

    public VertxMetricsConfigurationImpl(boolean enabled,
                                         boolean exposeViaJMX,
                                         String jmxDomainName)
    {
        this.enabled = enabled;
        this.exposeViaJMX = exposeViaJMX;
        this.jmxDomainName = jmxDomainName;
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
}
