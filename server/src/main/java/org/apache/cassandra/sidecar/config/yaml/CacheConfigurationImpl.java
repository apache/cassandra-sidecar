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
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Configuration class that encapsulates parameters needed for Caches
 */
public class CacheConfigurationImpl implements CacheConfiguration
{
    @JsonProperty("expire_after_access_millis")
    protected final long expireAfterAccessMillis;

    @JsonProperty("maximum_size")
    protected final long maximumSize;

    @JsonProperty(value = "enabled")
    protected final boolean enabled;

    @JsonProperty("warmup_retries")
    protected final int warmupRetries;

    @JsonProperty("warmup_retry_interval_millis")
    protected final long warmupRetryIntervalMillis;

    public CacheConfigurationImpl()
    {
        this(TimeUnit.HOURS.toMillis(1), 100, true, 5, 1000);
    }

    @VisibleForTesting
    public CacheConfigurationImpl(long expireAfterAccessMillis, long maximumSize)
    {
        this(expireAfterAccessMillis, maximumSize, true, 5, 1000);
    }

    public CacheConfigurationImpl(long expireAfterAccessMillis, long maximumSize, boolean enabled, int warmupRetries, long warmupRetryIntervalMillis)
    {
        this.expireAfterAccessMillis = expireAfterAccessMillis;
        this.maximumSize = maximumSize;
        this.enabled = enabled;
        this.warmupRetries = warmupRetries;
        this.warmupRetryIntervalMillis = warmupRetryIntervalMillis;
    }

    @Override
    @JsonProperty("expire_after_access_millis")
    public long expireAfterAccessMillis()
    {
        return expireAfterAccessMillis;
    }

    @Override
    @JsonProperty("maximum_size")
    public long maximumSize()
    {
        return maximumSize;
    }

    @JsonProperty(value = "enabled")
    @Override
    public boolean enabled()
    {
        return enabled;
    }

    @JsonProperty(value = "warmup_retries")
    public int warmupRetries()
    {
        return warmupRetries;
    }

    @JsonProperty(value = "warmup_retry_interval_millis")
    public long warmupRetryIntervalMillis()
    {
        return warmupRetryIntervalMillis;
    }
}
