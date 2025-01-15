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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.S3ProxyConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulates configuration needed to create S3 client
 */
public class S3ClientConfigurationImpl implements S3ClientConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ClientConfigurationImpl.class);
    public static final String DEFAULT_THREAD_NAME_PREFIX = "s3-client";

    public static final String PROXY_PROPERTY = "proxy_config";
    public static final String API_CALL_TIMEOUT = "api_call_timeout";
    public static final String RANGE_GET_OBJECT_BYTES_SIZE = "range_get_object_bytes_size";
    public static final String THREAD_KEEP_ALIVE = "thread_keep_alive";
    public static final String CONCURRENCY = "concurrency";
    public static final String THREAD_NAME_PREFIX = "thread_name_prefix";

    public static final SecondBoundConfiguration DEFAULT_THREAD_KEEP_ALIVE = SecondBoundConfiguration.parse("60s");
    public static final int DEFAULT_S3_CLIENT_CONCURRENCY = 4;
    public static final MillisecondBoundConfiguration DEFAULT_API_CALL_TIMEOUT = MillisecondBoundConfiguration.parse("60s");
    public static final MillisecondBoundConfiguration MINIMUM_API_CALL_TIMEOUT = MillisecondBoundConfiguration.parse("10s");
    public static final int DEFAULT_RANGE_GET_OBJECT_BYTES_SIZE = 5 * 1024 * 1024; // 5 MiB

    @JsonProperty(value = THREAD_NAME_PREFIX)
    protected final String threadNamePrefix;

    @JsonProperty(value = CONCURRENCY)
    protected final int concurrency;

    protected SecondBoundConfiguration threadKeepAlive;

    @JsonProperty(value = RANGE_GET_OBJECT_BYTES_SIZE)
    protected final int rangeGetObjectBytesSize;

    protected MillisecondBoundConfiguration apiCallTimeout;

    @JsonProperty(value = PROXY_PROPERTY)
    protected final S3ProxyConfiguration proxyConfig;

    public S3ClientConfigurationImpl()
    {
        this(DEFAULT_THREAD_NAME_PREFIX,
             DEFAULT_S3_CLIENT_CONCURRENCY,
             DEFAULT_THREAD_KEEP_ALIVE,
             DEFAULT_RANGE_GET_OBJECT_BYTES_SIZE,
             DEFAULT_API_CALL_TIMEOUT,
             new S3ProxyConfigurationImpl());
    }

    public S3ClientConfigurationImpl(String threadNamePrefix,
                                     int concurrency,
                                     SecondBoundConfiguration threadKeepAlive,
                                     int rangeGetObjectBytesSize,
                                     MillisecondBoundConfiguration apiCallTimeout,
                                     S3ProxyConfiguration proxyConfig)
    {
        Preconditions.checkArgument(apiCallTimeout.compareTo(MINIMUM_API_CALL_TIMEOUT) > 0,
                                    () -> String.format("apiCallTimeout cannot be smaller than %s. Configured: %s",
                                                        MINIMUM_API_CALL_TIMEOUT, apiCallTimeout));
        this.threadNamePrefix = threadNamePrefix;
        this.concurrency = concurrency;
        this.threadKeepAlive = threadKeepAlive;
        this.rangeGetObjectBytesSize = rangeGetObjectBytesSize;
        this.proxyConfig = proxyConfig;
        this.apiCallTimeout = apiCallTimeout;
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    @JsonProperty(value = THREAD_NAME_PREFIX)
    public String threadNamePrefix()
    {
        return threadNamePrefix;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = CONCURRENCY)
    public int concurrency()
    {
        return concurrency;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = THREAD_KEEP_ALIVE)
    public SecondBoundConfiguration threadKeepAlive()
    {
        return threadKeepAlive;
    }

    @JsonProperty(value = THREAD_KEEP_ALIVE)
    public void setThreadKeepAlive(SecondBoundConfiguration threadKeepAlive)
    {
        this.threadKeepAlive = threadKeepAlive;
    }

    /**
     * Legacy property {@code thread_keep_alive_seconds}
     *
     * @param threadKeepAliveSeconds keep alive time in seconds
     * @deprecated in favor of {@link #THREAD_KEEP_ALIVE}
     */
    @JsonProperty(value = "thread_keep_alive_seconds")
    @Deprecated
    public void setThreadKeepAliveSeconds(long threadKeepAliveSeconds)
    {
        LOGGER.warn("'thread_keep_alive_seconds' is deprecated, use '{}' instead", THREAD_KEEP_ALIVE);
        setThreadKeepAlive(new SecondBoundConfiguration(threadKeepAliveSeconds, TimeUnit.SECONDS));
    }

    @Override
    @JsonProperty(value = RANGE_GET_OBJECT_BYTES_SIZE)
    public int rangeGetObjectBytesSize()
    {
        return rangeGetObjectBytesSize;
    }

    @Override
    @JsonProperty(value = API_CALL_TIMEOUT)
    public MillisecondBoundConfiguration apiCallTimeout()
    {
        return apiCallTimeout;
    }

    @JsonProperty(value = API_CALL_TIMEOUT)
    public void setApiCallTimeout(MillisecondBoundConfiguration apiCallTimeout)
    {
        this.apiCallTimeout = apiCallTimeout;
    }

    /**
     * Legacy property {@code api_call_timeout_millis}
     *
     * @param apiCallTimeoutMillis timeout in milliseconds
     * @deprecated in favor of {@code api_call_timeout}
     */
    @JsonProperty(value = "api_call_timeout_millis")
    @Deprecated
    public void setApiCallTimeoutMillis(long apiCallTimeoutMillis)
    {
        LOGGER.warn("'api_call_timeout_millis' is deprecated, use 'api_call_timeout' instead");
        setApiCallTimeout(new MillisecondBoundConfiguration(apiCallTimeoutMillis, TimeUnit.MILLISECONDS));
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
