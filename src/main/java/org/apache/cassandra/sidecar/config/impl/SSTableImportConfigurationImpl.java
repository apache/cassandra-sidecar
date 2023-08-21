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
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;

/**
 * Configuration for the SSTable Import functionality
 */
public class SSTableImportConfigurationImpl implements SSTableImportConfiguration
{
    public static final String POLL_INTERVAL_MILLIS_PROPERTY = "poll_interval_millis";
    public static final int DEFAULT_POLL_INTERVAL_MILLIS = 100;
    public static final String CACHE_PROPERTY = "cache";
    protected static final CacheConfiguration DEFAULT_CACHE_CONFIGURATION =
    CacheConfigurationImpl.builder()
                          .expireAfterAccessMillis(TimeUnit.HOURS.toMillis(2))
                          .maximumSize(10_000)
                          .build();


    @JsonProperty(value = POLL_INTERVAL_MILLIS_PROPERTY, defaultValue = DEFAULT_POLL_INTERVAL_MILLIS + "")
    private final int importIntervalMillis;

    @JsonProperty(value = CACHE_PROPERTY)
    private final CacheConfiguration cacheConfiguration;

    public SSTableImportConfigurationImpl()
    {
        importIntervalMillis = DEFAULT_POLL_INTERVAL_MILLIS;
        cacheConfiguration = DEFAULT_CACHE_CONFIGURATION;
    }

    protected SSTableImportConfigurationImpl(Builder<?> builder)
    {
        importIntervalMillis = builder.importIntervalMillis;
        cacheConfiguration = builder.cacheConfiguration;
    }

    /**
     * @return the interval in milliseconds in which the SSTable Importer will process pending imports
     */
    @Override
    @JsonProperty(value = POLL_INTERVAL_MILLIS_PROPERTY, defaultValue = DEFAULT_POLL_INTERVAL_MILLIS + "")
    public int importIntervalMillis()
    {
        return importIntervalMillis;
    }

    /**
     * @return the configuration for the cache used for SSTable Import requests
     */
    @Override
    @JsonProperty(value = CACHE_PROPERTY)
    public CacheConfiguration cacheConfiguration()
    {
        return cacheConfiguration;
    }

    public static Builder<?> builder()
    {
        return new Builder<>();
    }

    /**
     * {@code SSTableImportConfigurationImpl} builder static inner class.
     * @param <T> the builder type
     */
    public static class Builder<T extends Builder<?>> implements DataObjectBuilder<T, SSTableImportConfigurationImpl>
    {
        protected int importIntervalMillis = DEFAULT_POLL_INTERVAL_MILLIS;
        protected CacheConfiguration cacheConfiguration = DEFAULT_CACHE_CONFIGURATION;

        protected Builder()
        {
        }

        /**
         * Sets the {@code importIntervalMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param importIntervalMillis the {@code importIntervalMillis} to set
         * @return a reference to this Builder
         */
        public T importIntervalMillis(int importIntervalMillis)
        {
            return override(b -> b.importIntervalMillis = importIntervalMillis);
        }

        /**
         * Sets the {@code cacheConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param cacheConfiguration the {@code cacheConfiguration} to set
         * @return a reference to this Builder
         */
        public T cacheConfiguration(CacheConfiguration cacheConfiguration)
        {
            return override(b -> b.cacheConfiguration = cacheConfiguration);
        }

        /**
         * Returns a {@code SSTableImportConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code SSTableImportConfigurationImpl} built with parameters of this
         * {@code SSTableImportConfigurationImpl.Builder}
         */
        @Override
        public SSTableImportConfigurationImpl build()
        {
            return new SSTableImportConfigurationImpl(this);
        }
    }
}
