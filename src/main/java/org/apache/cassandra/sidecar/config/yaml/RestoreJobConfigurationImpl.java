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
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;

/**
 * Configuration needed restore jobs restoring data from blob
 */
public class RestoreJobConfigurationImpl implements RestoreJobConfiguration
{
    private static final long MIN_RESTORE_JOB_TABLES_TTL_SECONDS = TimeUnit.DAYS.toSeconds(14);

    public static final long DEFAULT_JOB_DISCOVERY_ACTIVE_LOOP_DELAY_MILLIS = TimeUnit.MINUTES.toMillis(5);
    public static final long DEFAULT_JOB_DISCOVERY_IDLE_LOOP_DELAY_MILLIS = TimeUnit.MINUTES.toMillis(10);
    public static final int DEFAULT_JOB_DISCOVERY_RECENCY_DAYS = 5;
    public static final int DEFAULT_PROCESS_MAX_CONCURRENCY = 20; // process at most 20 slices concurrently
    public static final long DEFAULT_RESTORE_JOB_TABLES_TTL_SECONDS = TimeUnit.DAYS.toSeconds(90);

    @JsonProperty(value = "job_discovery_active_loop_delay_millis")
    protected final long jobDiscoveryActiveLoopDelayMillis;

    @JsonProperty(value = "job_discovery_idle_loop_delay_millis")
    protected final long jobDiscoveryIdleLoopDelayMillis;

    @JsonProperty(value = "job_discovery_recency_days")
    protected final int jobDiscoveryRecencyDays;

    @JsonProperty(value = "slice_process_max_concurrency")
    protected final int processMaxConcurrency;

    @JsonProperty(value = "restore_job_tables_ttl_seconds")
    protected final long restoreJobTablesTtlSeconds;

    protected RestoreJobConfigurationImpl()
    {
        this(builder());
    }

    protected RestoreJobConfigurationImpl(Builder builder)
    {
        this.jobDiscoveryActiveLoopDelayMillis = builder.jobDiscoveryActiveLoopDelayMillis;
        this.jobDiscoveryIdleLoopDelayMillis = builder.jobDiscoveryIdleLoopDelayMillis;
        this.jobDiscoveryRecencyDays = builder.jobDiscoveryRecencyDays;
        this.processMaxConcurrency = builder.processMaxConcurrency;
        this.restoreJobTablesTtlSeconds = builder.restoreJobTablesTtlSeconds;
        validate();
    }

    private void validate()
    {
        long ttl = restoreJobTablesTtlSeconds();
        if (ttl < MIN_RESTORE_JOB_TABLES_TTL_SECONDS)
        {
            throw new IllegalArgumentException("restoreJobTablesTtl cannot be less than "
                                               + MIN_RESTORE_JOB_TABLES_TTL_SECONDS);
        }
        if (TimeUnit.DAYS.toSeconds(jobDiscoveryRecencyDays()) >= ttl)
        {
            throw new IllegalArgumentException("JobDiscoveryRecencyDays (in seconds) cannot be greater than "
                                               + ttl);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "job_discovery_active_loop_delay_millis")
    public long jobDiscoveryActiveLoopDelayMillis()
    {
        return jobDiscoveryActiveLoopDelayMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "job_discovery_idle_loop_delay_millis")
    public long jobDiscoveryIdleLoopDelayMillis()
    {
        return jobDiscoveryActiveLoopDelayMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "job_discovery_recency_days")
    public int jobDiscoveryRecencyDays()
    {
        return jobDiscoveryRecencyDays;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "slice_process_max_concurrency")
    public int processMaxConcurrency()
    {
        return processMaxConcurrency;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "restore_job_tables_ttl_seconds")
    public long restoreJobTablesTtlSeconds()
    {
        return restoreJobTablesTtlSeconds;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code RestoreJobConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, RestoreJobConfigurationImpl>
    {
        private long jobDiscoveryActiveLoopDelayMillis = DEFAULT_JOB_DISCOVERY_ACTIVE_LOOP_DELAY_MILLIS;
        private long jobDiscoveryIdleLoopDelayMillis = DEFAULT_JOB_DISCOVERY_IDLE_LOOP_DELAY_MILLIS;
        private int jobDiscoveryRecencyDays = DEFAULT_JOB_DISCOVERY_RECENCY_DAYS;
        private int processMaxConcurrency = DEFAULT_PROCESS_MAX_CONCURRENCY;
        private long restoreJobTablesTtlSeconds = DEFAULT_RESTORE_JOB_TABLES_TTL_SECONDS;

        protected Builder()
        {
        }

        @Override
        public RestoreJobConfigurationImpl.Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code jobDiscoveryActiveLoopDelayMillis} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param jobDiscoveryActiveLoopDelayMillis the {@code jobDiscoveryActiveLoopDelayMillis} to set
         * @return a reference to this Builder
         */
        public Builder jobDiscoveryActiveLoopDelayMillis(long jobDiscoveryActiveLoopDelayMillis)
        {
            return update(b -> b.jobDiscoveryActiveLoopDelayMillis = jobDiscoveryActiveLoopDelayMillis);
        }

        /**
         * Sets the {@code jobDiscoveryIdleLoopDelayMillis} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param jobDiscoveryIdleLoopDelayMillis the {@code jobDiscoveryIdleLoopDelayMillis} to set
         * @return a reference to this Builder
         */
        public Builder jobDiscoveryIdleLoopDelayMillis(long jobDiscoveryIdleLoopDelayMillis)
        {
            return update(b -> b.jobDiscoveryIdleLoopDelayMillis = jobDiscoveryIdleLoopDelayMillis);
        }

        /**
         * Sets the {@code jobDiscoveryRecencyDays} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param jobDiscoveryRecencyDays the {@code jobDiscoveryRecencyDays} to set
         * @return a reference to this Builder
         */
        public Builder jobDiscoveryRecencyDays(int jobDiscoveryRecencyDays)
        {
            return update(b -> b.jobDiscoveryRecencyDays = jobDiscoveryRecencyDays);
        }

        /**
         * Sets the {@code processMaxConcurrency} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param processMaxConcurrency the {@code processMaxConcurrency} to set
         * @return a reference to this Builder
         */
        public Builder processMaxConcurrency(int processMaxConcurrency)
        {
            return update(b -> b.processMaxConcurrency = processMaxConcurrency);
        }

        /**
         * Sets the {@code restoreJobTablesTtlSeconds} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param restoreJobTablesTtlSeconds the {@code restoreJobTablesTtlSeconds} to set
         * @return a reference to this Builder
         */
        public Builder restoreJobTablesTtlSeconds(long restoreJobTablesTtlSeconds)
        {
            return update(b -> b.restoreJobTablesTtlSeconds = restoreJobTablesTtlSeconds);
        }

        @Override
        public RestoreJobConfigurationImpl build()
        {
            return new RestoreJobConfigurationImpl(this);
        }
    }
}
