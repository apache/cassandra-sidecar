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
import org.apache.cassandra.sidecar.config.RefreshPermissionCachesConfiguration;

/**
 * {@inheritDoc}
 */
public class RefreshPermissionCachesConfigurationImpl implements RefreshPermissionCachesConfiguration
{
    public static final String INITIAL_DELAY_MILLIS_PROPERTY = "initial_delay_millis";
    public static final int DEFAULT_INITIAL_DELAY_MILLIS = 0;

    public static final String POLL_FREQ_MILLIS_PROPERTY = "poll_freq_millis";
    public static final int DEFAULT_CHECK_INTERVAL_MILLIS = 3000;

    public static final String STALE_AFTER_MILLIS_PROPERTY = "stale_after_access_millis";
    public static final int DEFAULT_STALE_AFTER_MILLIS = 3600000;

    public static final String CACHE_PROPERTY = "cache";
    protected static final CacheConfiguration DEFAULT_CACHE_CONFIGURATION =
    new CacheConfigurationImpl(TimeUnit.HOURS.toMillis(2), 10_000);

    @JsonProperty(value = INITIAL_DELAY_MILLIS_PROPERTY)
    protected final int initialDelayMillis;

    @JsonProperty(value = POLL_FREQ_MILLIS_PROPERTY)
    protected final int checkIntervalMillis;

    @JsonProperty(value = STALE_AFTER_MILLIS_PROPERTY)
    protected final int staleAfterMillis;

    @JsonProperty(value = CACHE_PROPERTY)
    protected final CacheConfiguration cacheConfiguration;

    public RefreshPermissionCachesConfigurationImpl()
    {
        this(DEFAULT_INITIAL_DELAY_MILLIS, DEFAULT_CHECK_INTERVAL_MILLIS,
             DEFAULT_STALE_AFTER_MILLIS, DEFAULT_CACHE_CONFIGURATION);
    }

    public RefreshPermissionCachesConfigurationImpl(int initialDelayMillis,
                                                    int checkIntervalMillis,
                                                    int staleAfterMillis,
                                                    CacheConfiguration cacheConfiguration)
    {
        this.initialDelayMillis = initialDelayMillis;
        this.checkIntervalMillis = checkIntervalMillis;
        this.staleAfterMillis = staleAfterMillis;
        this.cacheConfiguration = cacheConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    public int initialDelayMillis()
    {
        return initialDelayMillis;
    }

    /**
     * {@inheritDoc}
     */
    public int checkIntervalMillis()
    {
        return checkIntervalMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = CACHE_PROPERTY)
    public CacheConfiguration cacheConfiguration()
    {
        return cacheConfiguration;
    }
}
