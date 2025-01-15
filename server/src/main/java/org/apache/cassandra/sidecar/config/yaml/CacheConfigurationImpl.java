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
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Configuration class that encapsulates parameters needed for Caches
 */
public class CacheConfigurationImpl implements CacheConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheConfigurationImpl.class);

    protected MillisecondBoundConfiguration expireAfterAccess;

    @JsonProperty("maximum_size")
    protected final long maximumSize;

    @JsonProperty(value = "enabled")
    protected final boolean enabled;

    @JsonProperty("warmup_retries")
    protected final int warmupRetries;

    protected MillisecondBoundConfiguration warmupRetryInterval;

    public CacheConfigurationImpl()
    {
        this(MillisecondBoundConfiguration.parse("1h"), 100, true, 5, MillisecondBoundConfiguration.parse("1s"));
    }

    @VisibleForTesting
    public CacheConfigurationImpl(MillisecondBoundConfiguration expireAfterAccess, long maximumSize)
    {
        this(expireAfterAccess, maximumSize, true, 5, MillisecondBoundConfiguration.parse("1s"));
    }

    public CacheConfigurationImpl(MillisecondBoundConfiguration expireAfterAccess,
                                  long maximumSize,
                                  boolean enabled,
                                  int warmupRetries,
                                  MillisecondBoundConfiguration warmupRetryInterval)
    {
        this.expireAfterAccess = expireAfterAccess;
        this.maximumSize = maximumSize;
        this.enabled = enabled;
        this.warmupRetries = warmupRetries;
        this.warmupRetryInterval = warmupRetryInterval;
    }

    @Override
    @JsonProperty("expire_after_access")
    public MillisecondBoundConfiguration expireAfterAccess()
    {
        return expireAfterAccess;
    }

    @JsonProperty("expire_after_access")
    public void setExpireAfterAccess(MillisecondBoundConfiguration expireAfterAccess)
    {
        this.expireAfterAccess = expireAfterAccess;
    }

    /**
     * Legacy property {@code expire_after_access_millis}
     *
     * @param expireAfterAccessMillis expiry in milliseconds
     * @deprecated in favor of {@code expire_after_access}
     */
    @JsonProperty("expire_after_access_millis")
    @Deprecated
    public void setExpireAfterAccessMillis(long expireAfterAccessMillis)
    {
        LOGGER.warn("'expire_after_access_millis' is deprecated, use 'expire_after_access' instead");
        setExpireAfterAccess(new MillisecondBoundConfiguration(expireAfterAccessMillis, TimeUnit.MILLISECONDS));
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

    @JsonProperty(value = "warmup_retry_interval")
    public MillisecondBoundConfiguration warmupRetryInterval()
    {
        return warmupRetryInterval;
    }

    @JsonProperty("warmup_retry_interval")
    public void setWarmupRetryInterval(MillisecondBoundConfiguration warmupRetryInterval)
    {
        this.warmupRetryInterval = warmupRetryInterval;
    }

    /**
     * Legacy property {@code warmup_retry_interval_millis}
     *
     * @param warmupRetryIntervalMillis interval in milliseconds
     * @deprecated in favor of {@code warmup_retry_interval}
     */
    @JsonProperty("warmup_retry_interval_millis")
    @Deprecated
    public void setWarmupRetryIntervalMillis(long warmupRetryIntervalMillis)
    {
        LOGGER.warn("'warmup_retry_interval_millis' is deprecated, use 'warmup_retry_interval' instead");
        setWarmupRetryInterval(new MillisecondBoundConfiguration(warmupRetryIntervalMillis, TimeUnit.MILLISECONDS));
    }
}
