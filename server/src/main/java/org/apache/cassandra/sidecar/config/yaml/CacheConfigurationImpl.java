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
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;

/**
 * Configuration class that encapsulates parameters needed for Caches
 */
public class CacheConfigurationImpl implements CacheConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheConfigurationImpl.class);
    private static final long DEFAULT_MAXIMUM_SIZE = 100;
    private static final boolean DEFAULT_ENABLED = true;
    private static final int DEFAULT_WARMUP_RETRIES = 5;
    private static final MillisecondBoundConfiguration DEFAULT_WARMUP_RETRY_INTERVAL = MillisecondBoundConfiguration.parse("1s");

    protected MillisecondBoundConfiguration expireAfterAccess;

    @JsonProperty("refresh_after_write")
    protected final MillisecondBoundConfiguration refreshAfterWrite;

    @JsonProperty("maximum_size")
    protected final long maximumSize;

    @JsonProperty(value = "enabled")
    protected final boolean enabled;

    @JsonProperty("warmup_retries")
    protected final int warmupRetries;

    protected MillisecondBoundConfiguration warmupRetryInterval;

    public CacheConfigurationImpl()
    {
        this(builder());
    }

    protected CacheConfigurationImpl(Builder builder)
    {
        setExpireAfterAccess(builder.expireAfterAccess);
        maximumSize = builder.maximumSize;
        enabled = builder.enabled;
        warmupRetries = builder.warmupRetries;
        setWarmupRetryInterval(builder.warmupRetryInterval);
        refreshAfterWrite = builder.refreshAfterWrite;
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

    @Override
    @JsonProperty("refresh_after_write")
    public MillisecondBoundConfiguration refreshAfterWrite()
    {
        return refreshAfterWrite;
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

    @Override
    @JsonProperty(value = "warmup_retries")
    public int warmupRetries()
    {
        return warmupRetries;
    }

    @Override
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

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code CacheConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, CacheConfigurationImpl>
    {
        private MillisecondBoundConfiguration expireAfterAccess;
        private MillisecondBoundConfiguration refreshAfterWrite;
        private long maximumSize = DEFAULT_MAXIMUM_SIZE;
        private boolean enabled = DEFAULT_ENABLED;
        private int warmupRetries = DEFAULT_WARMUP_RETRIES;
        private MillisecondBoundConfiguration warmupRetryInterval = DEFAULT_WARMUP_RETRY_INTERVAL;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code expireAfterAccess} and returns a reference to this Builder enabling method chaining.
         *
         * @param expireAfterAccess the {@code expireAfterAccess} to set
         * @return a reference to this Builder
         */
        public Builder expireAfterAccess(MillisecondBoundConfiguration expireAfterAccess)
        {
            return update(b -> b.expireAfterAccess = expireAfterAccess);
        }

        /**
         * Sets the {@code refreshAfterWrite} and returns a reference to this Builder enabling method chaining.
         *
         * @param refreshAfterWrite the {@code refreshAfterWrite} to set
         * @return a reference to this Builder
         */
        public Builder refreshAfterWrite(MillisecondBoundConfiguration refreshAfterWrite)
        {
            return update(b -> b.refreshAfterWrite = refreshAfterWrite);
        }

        /**
         * Sets the {@code maximumSize} and returns a reference to this Builder enabling method chaining.
         *
         * @param maximumSize the {@code maximumSize} to set
         * @return a reference to this Builder
         */
        public Builder maximumSize(long maximumSize)
        {
            return update(b -> b.maximumSize = maximumSize);
        }

        /**
         * Sets the {@code enabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param enabled the {@code enabled} to set
         * @return a reference to this Builder
         */
        public Builder enabled(boolean enabled)
        {
            return update(b -> b.enabled = enabled);
        }

        /**
         * Sets the {@code warmupRetries} and returns a reference to this Builder enabling method chaining.
         *
         * @param warmupRetries the {@code warmupRetries} to set
         * @return a reference to this Builder
         */
        public Builder warmupRetries(int warmupRetries)
        {
            return update(b -> b.warmupRetries = warmupRetries);
        }

        /**
         * Sets the {@code warmupRetryInterval} and returns a reference to this Builder enabling method chaining.
         *
         * @param warmupRetryInterval the {@code warmupRetryInterval} to set
         * @return a reference to this Builder
         */
        public Builder warmupRetryInterval(MillisecondBoundConfiguration warmupRetryInterval)
        {
            return update(b -> b.warmupRetryInterval = warmupRetryInterval);
        }

        /**
         * Returns a {@code CacheConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code CacheConfigurationImpl} built with parameters of this {@code CacheConfigurationImpl.Builder}
         */
        @Override
        public CacheConfigurationImpl build()
        {
            return new CacheConfigurationImpl(this);
        }
    }
}
