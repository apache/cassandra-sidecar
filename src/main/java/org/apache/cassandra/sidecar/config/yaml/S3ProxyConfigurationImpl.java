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

import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.S3ProxyConfiguration;

/**
 * Configuration needed to create S3 proxy
 */
public class S3ProxyConfigurationImpl implements S3ProxyConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ProxyConfigurationImpl.class);

    @JsonProperty(value = "proxy")
    protected final String proxy;

    @JsonProperty(value = "username")
    protected final String username;

    @JsonProperty(value = "password")
    protected final String password;

    @JsonProperty("endpoint_override")
    protected final String endpointOverride;

    protected S3ProxyConfigurationImpl()
    {
        this(null, null, null, null);
    }

    protected S3ProxyConfigurationImpl(String proxy, String username, String password, String endpointOverride)
    {
        this.proxy = proxy;
        this.username = username;
        this.password = password;
        this.endpointOverride = endpointOverride;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "proxy")
    public URI proxy()
    {
        return toURI(proxy, "S3 client proxy");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "username")
    public String username()
    {
        return username;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "password")
    public String password()
    {
        return password;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @VisibleForTesting
    @JsonProperty("endpoint_override")
    public URI endpointOverride()
    {
        return toURI(endpointOverride, "S3 client endpointOverrides");
    }

    private URI toURI(String uriString, String hint)
    {
        if (uriString == null)
        {
            return null;
        }
        else
        {
            try
            {
                return new URI(uriString);
            }
            catch (URISyntaxException e)
            {
                LOGGER.error("{} is specified, but the value is invalid. input={}", hint, uriString);
                throw new RuntimeException("Unable to resolve " + uriString, e);
            }
        }
    }
}
