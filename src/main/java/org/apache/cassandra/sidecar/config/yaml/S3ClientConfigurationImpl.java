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
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.S3ProxyConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulates configuration needed to create S3 client
 */
@Binds(to = S3ClientConfiguration.class)
public class S3ClientConfigurationImpl implements S3ClientConfiguration
{
    public static final String DEFAULT_THREAD_NAME_PREFIX = "s3-client";

    public static final String PROXY_PROPERTY = "proxy_config";
    public static final long DEFAULT_THREAD_KEEP_ALIVE_SECONDS = 60;
    public static final int DEFAULT_S3_CLIENT_CONCURRENCY = 4;

    @JsonProperty(value = "thread_name_prefix", defaultValue = "s3-client")
    protected final String threadNamePrefix;

    @JsonProperty(value = "concurrency", defaultValue = "4")
    protected final int concurrency;

    @JsonProperty(value = "thread_keep_alive_seconds", defaultValue = "60")
    protected final long threadKeepAliveSeconds;

    @JsonProperty(value = PROXY_PROPERTY)
    protected final S3ProxyConfiguration proxyConfig;

    public S3ClientConfigurationImpl()
    {
        this(DEFAULT_THREAD_NAME_PREFIX,
             DEFAULT_S3_CLIENT_CONCURRENCY,
             DEFAULT_THREAD_KEEP_ALIVE_SECONDS,
             new S3ProxyConfigurationImpl());
    }

    public S3ClientConfigurationImpl(String threadNamePrefix,
                                     int concurrency,
                                     long threadKeepAliveSeconds,
                                     S3ProxyConfiguration proxyConfig)
    {
        this.threadNamePrefix = threadNamePrefix;
        this.concurrency = concurrency;
        this.threadKeepAliveSeconds = threadKeepAliveSeconds;
        this.proxyConfig = proxyConfig;
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    @JsonProperty(value = "thread_name_prefix", defaultValue = "s3-client")
    public String threadNamePrefix()
    {
        return threadNamePrefix;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "concurrency", defaultValue = "4")
    public int concurrency()
    {
        return concurrency;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "thread_keep_alive_seconds", defaultValue = "60")
    public long threadKeepAliveSeconds()
    {
        return threadKeepAliveSeconds;
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    @JsonProperty(value = PROXY_PROPERTY)
    public S3ProxyConfiguration proxyConfig()
    {
        return proxyConfig;
    }
}
