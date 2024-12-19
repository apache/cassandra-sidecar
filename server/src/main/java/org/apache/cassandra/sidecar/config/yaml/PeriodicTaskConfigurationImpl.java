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
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;

/**
 * Configuration for the {@link org.apache.cassandra.sidecar.tasks.PeriodicTask}
 */
public class PeriodicTaskConfigurationImpl implements PeriodicTaskConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskConfigurationImpl.class);
    private static final boolean DEFAULT_ENABLED = false;
    private static final long DEFAULT_EXECUTE_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long DEFAULT_INITIAL_DELAY_MILLIS = DEFAULT_EXECUTE_INTERVAL_MILLIS;

    @JsonProperty("enabled")
    private final boolean enabled;
    private long initialDelayMillis;
    private long executeIntervalMillis;

    public PeriodicTaskConfigurationImpl()
    {
        this(DEFAULT_ENABLED, DEFAULT_INITIAL_DELAY_MILLIS, DEFAULT_EXECUTE_INTERVAL_MILLIS);
    }

    public PeriodicTaskConfigurationImpl(boolean enabled, long initialDelayMillis, long executeIntervalMillis)
    {
        this.enabled = enabled;
        this.initialDelayMillis = initialDelayMillis;
        this.executeIntervalMillis = executeIntervalMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("enabled")
    public boolean enabled()
    {
        return enabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("initial_delay_millis")
    public long initialDelayMillis()
    {
        return initialDelayMillis;
    }

    @JsonProperty("initial_delay_millis")
    public void setInitialDelayMillis(long initialDelayMillis)
    {
        if (initialDelayMillis > 0)
        {
            this.initialDelayMillis = initialDelayMillis;
        }
        else
        {
            LOGGER.warn("Invalid initialDelayMillis configuration {}, the minimum value is 0", initialDelayMillis);
            this.initialDelayMillis = 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("execute_interval_millis")
    public long executeIntervalMillis()
    {
        return executeIntervalMillis;
    }

    @JsonProperty("execute_interval_millis")
    public void setExecuteIntervalMillis(long executeIntervalMillis)
    {
        if (executeIntervalMillis > 1)
        {
            this.executeIntervalMillis = executeIntervalMillis;
        }
        else
        {
            LOGGER.warn("Invalid executeIntervalMillis configuration {}, the minimum value is 1", executeIntervalMillis);
            this.executeIntervalMillis = 1;
        }
    }
}
