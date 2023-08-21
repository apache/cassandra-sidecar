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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;

/**
 * Configuration for the health checks
 */
public class HealthCheckConfigurationImpl implements HealthCheckConfiguration
{
    public static final String POLL_FREQ_MILLIS_PROPERTY = "poll_freq_millis";
    public static final int DEFAULT_CHECK_INTERVAL_MILLIS = 30000;

    @JsonProperty(value = POLL_FREQ_MILLIS_PROPERTY, defaultValue = DEFAULT_CHECK_INTERVAL_MILLIS + "")
    private final int checkIntervalMillis;

    public HealthCheckConfigurationImpl()
    {
        checkIntervalMillis = DEFAULT_CHECK_INTERVAL_MILLIS;
    }

    protected HealthCheckConfigurationImpl(Builder<?> builder)
    {
        checkIntervalMillis = builder.checkIntervalMillis;
    }

    /**
     * @return the interval, in milliseconds, in which the health checks will be performed
     */
    @Override
    @JsonProperty(value = POLL_FREQ_MILLIS_PROPERTY, defaultValue = DEFAULT_CHECK_INTERVAL_MILLIS + "")
    public int checkIntervalMillis()
    {
        return checkIntervalMillis;
    }

    public static Builder<?> builder()
    {
        return new Builder<>();
    }

    /**
     * {@code HealthCheckConfigurationImpl} builder static inner class.
     * @param <T> the builder type
     */
    public static class Builder<T extends Builder<?>> implements DataObjectBuilder<T, HealthCheckConfigurationImpl>
    {
        protected int checkIntervalMillis = DEFAULT_CHECK_INTERVAL_MILLIS;

        protected Builder()
        {
        }

        /**
         * Sets the {@code checkIntervalMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param checkIntervalMillis the {@code checkIntervalMillis} to set
         * @return a reference to this Builder
         */
        public T checkIntervalMillis(int checkIntervalMillis)
        {
            return override(b -> b.checkIntervalMillis = checkIntervalMillis);
        }

        /**
         * Returns a {@code HealthCheckConfiguration} built from the parameters previously set.
         *
         * @return a {@code HealthCheckConfiguration} built with parameters of this
         * {@code HealthCheckConfiguration.Builder}
         */
        @Override
        public HealthCheckConfigurationImpl build()
        {
            return new HealthCheckConfigurationImpl(this);
        }
    }
}
