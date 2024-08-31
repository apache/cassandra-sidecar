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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.S3ProxyConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulates configuration needed to create S3 client
 */
public class S3ClientConfigurationImpl implements S3ClientConfiguration
{
    public static final String DEFAULT_THREAD_NAME_PREFIX = "s3-client";

    public static final String PROXY_PROPERTY = "proxy_config";
    public static final String API_CALL_TIMEOUT_MILLIS = "api_call_timeout_millis";
    public static final String RANGE_GET_OBJECT_BYTES_SIZE = "range_get_object_bytes_size";
    public static final String THREAD_KEEP_ALIVE_SECONDS = "thread_keep_alive_seconds";
    public static final String CONCURRENCY = "concurrency";
    public static final String THREAD_NAME_PREFIX = "thread_name_prefix";

    public static final long DEFAULT_THREAD_KEEP_ALIVE_SECONDS = 60;
    public static final int DEFAULT_S3_CLIENT_CONCURRENCY = 4;
    public static final long DEFAULT_API_CALL_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);
    public static final int DEFAULT_RANGE_GET_OBJECT_BYTES_SIZE = 5 * 1024 * 1024; // 5 MiB

    @JsonProperty(value = THREAD_NAME_PREFIX)
    protected final String threadNamePrefix;

    @JsonProperty(value = CONCURRENCY)
    protected final int concurrency;

    @JsonProperty(value = THREAD_KEEP_ALIVE_SECONDS)
    protected final long threadKeepAliveSeconds;

    @JsonProperty(value = RANGE_GET_OBJECT_BYTES_SIZE)
    protected final int rangeGetObjectBytesSize;

    @JsonProperty(value = API_CALL_TIMEOUT_MILLIS)
    protected final long apiCallTimeoutMillis;

    @JsonProperty(value = PROXY_PROPERTY)
    protected final S3ProxyConfiguration proxyConfig;

    public S3ClientConfigurationImpl()
    {
        this(DEFAULT_THREAD_NAME_PREFIX,
             DEFAULT_S3_CLIENT_CONCURRENCY,
             DEFAULT_THREAD_KEEP_ALIVE_SECONDS,
             DEFAULT_RANGE_GET_OBJECT_BYTES_SIZE,
             DEFAULT_API_CALL_TIMEOUT_MILLIS,
             new S3ProxyConfigurationImpl());
    }

    public S3ClientConfigurationImpl(String threadNamePrefix,
                                     int concurrency,
                                     long threadKeepAliveSeconds,
                                     int rangeGetObjectBytesSize,
                                     long apiCallTimeoutMillis,
                                     S3ProxyConfiguration proxyConfig)
    {
        Preconditions.checkArgument(apiCallTimeoutMillis > TimeUnit.SECONDS.toMillis(10),
                                    "apiCallTimeout cannot be smaller than 10 seconds. Configured: " + apiCallTimeoutMillis + " ms");
        this.threadNamePrefix = threadNamePrefix;
        this.concurrency = concurrency;
        this.threadKeepAliveSeconds = threadKeepAliveSeconds;
        this.rangeGetObjectBytesSize = rangeGetObjectBytesSize;
        this.proxyConfig = proxyConfig;
        this.apiCallTimeoutMillis = apiCallTimeoutMillis;
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
    @JsonProperty(value = THREAD_KEEP_ALIVE_SECONDS)
    public long threadKeepAliveSeconds()
    {
        return threadKeepAliveSeconds;
    }

    @Override
    @JsonProperty(value = RANGE_GET_OBJECT_BYTES_SIZE)
    public int rangeGetObjectBytesSize()
    {
        return rangeGetObjectBytesSize;
    }

    @Override
    @JsonProperty(value = API_CALL_TIMEOUT_MILLIS)
    public long apiCallTimeoutMillis()
    {
        return apiCallTimeoutMillis;
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
