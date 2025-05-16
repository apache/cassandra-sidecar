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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration.ONE;

/**
 * Configuration for the {@link PeriodicTask}
 */
public class PeriodicTaskConfigurationImpl implements PeriodicTaskConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskConfigurationImpl.class);
    private static final boolean DEFAULT_ENABLED = false;

    @JsonProperty("enabled")
    private final boolean enabled;
    private MillisecondBoundConfiguration initialDelay;
    private MillisecondBoundConfiguration executeInterval;

    /**
     * Default constructor for jackson deserialization and allows to set the fields, including the aliased ones.
     */
    public PeriodicTaskConfigurationImpl()
    {
        this.enabled = DEFAULT_ENABLED;
    }

    public PeriodicTaskConfigurationImpl(boolean enabled,
                                         MillisecondBoundConfiguration initialDelay,
                                         MillisecondBoundConfiguration executeInterval)
    {
        this.enabled = enabled;
        this.initialDelay = initialDelay;
        this.executeInterval = executeInterval;
    }

    protected PeriodicTaskConfigurationImpl(Builder builder)
    {
        enabled = builder.enabled;
        setInitialDelay(builder.initialDelay);
        setExecuteInterval(Objects.requireNonNull(builder.executeInterval, "executeInterval must be configured"));
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
    @JsonProperty("initial_delay")
    @NotNull
    public MillisecondBoundConfiguration initialDelay()
    {
        if (initialDelay == null)
        {
            return executeInterval;
        }
        return initialDelay;
    }

    @JsonProperty("initial_delay")
    public void setInitialDelay(MillisecondBoundConfiguration initialDelay)
    {
        if (initialDelay != null)
        {
            if (initialDelay.compareTo(MillisecondBoundConfiguration.ZERO) >= 0)
            {
                this.initialDelay = initialDelay;
            }
            else
            {
                LOGGER.warn("Invalid initialDelay configuration {}, the minimum value is 0", initialDelay);
                this.initialDelay = MillisecondBoundConfiguration.ZERO;
            }
        }
    }

    /**
     * Legacy property {@code initial_delay_millis}
     *
     * @param initialDelayMillis initial delay in milliseconds
     * @deprecated in favor of {@code initial_delay}
     */
    @JsonProperty("initial_delay_millis")
    @Deprecated
    public void setInitialDelayMillis(long initialDelayMillis)
    {
        LOGGER.warn("'initial_delay_millis' is deprecated, use 'initial_delay' instead");
        setInitialDelay(new MillisecondBoundConfiguration(initialDelayMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("execute_interval")
    @NotNull
    public MillisecondBoundConfiguration executeInterval()
    {
        return executeInterval;
    }

    @JsonProperty("execute_interval")
    public void setExecuteInterval(MillisecondBoundConfiguration executeInterval)
    {
        if (executeInterval.compareTo(ONE) > 0)
        {
            this.executeInterval = executeInterval;
        }
        else
        {
            LOGGER.warn("Invalid executeInterval configuration {}, the minimum value is 1ms", executeInterval);
            this.executeInterval = ONE;
        }
    }

    /**
     * Legacy properties {@code execute_interval_millis}, {@code poll_freq_millis}, and {@code poll_interval_millis}
     *
     * @param executeIntervalMillis the interval in milliseconds
     * @deprecated in favor of {@code execute_interval}
     */
    @JsonAlias({ "execute_interval_millis", "poll_freq_millis", "poll_interval_millis" })
    @Deprecated
    public void setExecuteIntervalMillis(long executeIntervalMillis)
    {
        LOGGER.warn("'execute_interval_millis', 'poll_freq_millis', and 'poll_interval_millis' are deprecated, " +
                    "use 'execute_interval' instead.");
        setExecuteInterval(new MillisecondBoundConfiguration(executeIntervalMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * {@code PeriodicTaskConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, PeriodicTaskConfigurationImpl>
    {
        private boolean enabled;
        private MillisecondBoundConfiguration initialDelay;
        private MillisecondBoundConfiguration executeInterval;

        private Builder()
        {
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @Override
        public Builder self()
        {
            return this;
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
         * Sets the {@code initialDelay} and returns a reference to this Builder enabling method chaining.
         *
         * @param initialDelay the {@code initialDelay} to set
         * @return a reference to this Builder
         */
        public Builder initialDelay(MillisecondBoundConfiguration initialDelay)
        {
            return update(b -> b.initialDelay = initialDelay);
        }

        /**
         * Sets the {@code executeInterval} and returns a reference to this Builder enabling method chaining.
         *
         * @param executeInterval the {@code executeInterval} to set
         * @return a reference to this Builder
         */
        public Builder executeInterval(MillisecondBoundConfiguration executeInterval)
        {
            return update(b -> b.executeInterval = executeInterval);
        }

        /**
         * Returns a {@code PeriodicTaskConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code PeriodicTaskConfigurationImpl} built with parameters of this {@code PeriodicTaskConfigurationImpl.Builder}
         */
        @Override
        public PeriodicTaskConfigurationImpl build()
        {
            return new PeriodicTaskConfigurationImpl(this);
        }
    }
}
