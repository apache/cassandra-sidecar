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

package org.apache.cassandra.sidecar.config.impl;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.CacheConfiguration;

/**
 * Configuration class that encapsulates parameters needed for Caches
 */
public class CacheConfigurationImpl implements CacheConfiguration
{
    @JsonProperty("expire_after_access_millis")
    private final long expireAfterAccessMillis;

    @JsonProperty("maximum_size")
    private final long maximumSize;

    public CacheConfigurationImpl()
    {
        expireAfterAccessMillis = TimeUnit.HOURS.toMillis(1);
        maximumSize = 100;
    }

    protected CacheConfigurationImpl(Builder builder)
    {
        expireAfterAccessMillis = builder.expireAfterAccessMillis;
        maximumSize = builder.maximumSize;
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

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code CacheConfiguration} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, CacheConfigurationImpl>
    {
        private long expireAfterAccessMillis = TimeUnit.HOURS.toMillis(1);
        private long maximumSize = 100;

        protected Builder()
        {
        }

        /**
         * Sets the {@code expireAfterAccessMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param expireAfterAccessMillis the {@code expireAfterAccessMillis} to set
         * @return a reference to this Builder
         */
        public Builder expireAfterAccessMillis(long expireAfterAccessMillis)
        {
            return update(b -> b.expireAfterAccessMillis = expireAfterAccessMillis);
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
         * Returns a {@code CacheConfiguration} built from the parameters previously set.
         *
         * @return a {@code CacheConfiguration} built with parameters of this {@code CacheConfiguration.Builder}
         */
        @Override
        public CacheConfigurationImpl build()
        {
            return new CacheConfigurationImpl(this);
        }
    }
}
