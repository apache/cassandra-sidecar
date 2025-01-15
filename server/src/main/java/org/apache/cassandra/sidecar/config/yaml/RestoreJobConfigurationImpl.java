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
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;

/**
 * Configuration needed restore jobs restoring data from blob
 */
public class RestoreJobConfigurationImpl implements RestoreJobConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobConfigurationImpl.class);
    private static final SecondBoundConfiguration MIN_RESTORE_JOB_TABLES_TTL =
    SecondBoundConfiguration.parse("14d");

    private static final MillisecondBoundConfiguration DEFAULT_JOB_DISCOVERY_ACTIVE_LOOP_DELAY =
    MillisecondBoundConfiguration.parse("5m");
    private static final MillisecondBoundConfiguration DEFAULT_JOB_DISCOVERY_IDLE_LOOP_DELAY =
    MillisecondBoundConfiguration.parse("10m");
    private static final int DEFAULT_JOB_DISCOVERY_MINIMUM_RECENCY_DAYS = 5;
    private static final int DEFAULT_PROCESS_MAX_CONCURRENCY = 20; // process at most 20 slices concurrently
    private static final SecondBoundConfiguration DEFAULT_RESTORE_JOB_TABLES_TTL =
    SecondBoundConfiguration.parse("90d");
    // A restore task is considered slow if it has been in the "active" list for 10 minutes.
    private static final SecondBoundConfiguration DEFAULT_RESTORE_JOB_SLOW_TASK_THRESHOLD = SecondBoundConfiguration.parse("10m");
    // report once a minute
    private static final SecondBoundConfiguration DEFAULT_RESTORE_JOB_SLOW_TASK_REPORT_DELAY = SecondBoundConfiguration.parse("1m");
    public static final MillisecondBoundConfiguration DEFAULT_RING_TOPOLOGY_REFRESH_DELAY = MillisecondBoundConfiguration.parse("1m");

    protected MillisecondBoundConfiguration jobDiscoveryActiveLoopDelay;

    protected MillisecondBoundConfiguration jobDiscoveryIdleLoopDelay;

    @JsonProperty(value = "job_discovery_minimum_recency_days")
    protected final int jobDiscoveryMinimumRecencyDays;

    @JsonProperty(value = "slice_process_max_concurrency")
    protected final int processMaxConcurrency;

    protected SecondBoundConfiguration restoreJobTablesTtl;

    protected SecondBoundConfiguration slowTaskThreshold;

    protected SecondBoundConfiguration slowTaskReportDelay;

    private MillisecondBoundConfiguration ringTopologyRefreshDelay;

    protected RestoreJobConfigurationImpl()
    {
        this(builder());
    }

    protected RestoreJobConfigurationImpl(Builder builder)
    {
        this.jobDiscoveryActiveLoopDelay = builder.jobDiscoveryActiveLoopDelay;
        this.jobDiscoveryIdleLoopDelay = builder.jobDiscoveryIdleLoopDelay;
        this.jobDiscoveryMinimumRecencyDays = builder.jobDiscoveryMinimumRecencyDays;
        this.processMaxConcurrency = builder.processMaxConcurrency;
        this.restoreJobTablesTtl = builder.restoreJobTablesTtl;
        this.slowTaskThreshold = builder.slowTaskThreshold;
        this.slowTaskReportDelay = builder.slowTaskReportDelay;
        this.ringTopologyRefreshDelay = builder.ringTopologyRefreshDelay;
        validate();
    }

    private void validate()
    {
        if (restoreJobTablesTtl.compareTo(MIN_RESTORE_JOB_TABLES_TTL) < 0)
        {
            throw new IllegalArgumentException("restoreJobTablesTtl cannot be less than "
                                               + MIN_RESTORE_JOB_TABLES_TTL);
        }
        long ttl = restoreJobTablesTtl().toSeconds();
        if (TimeUnit.DAYS.toSeconds(jobDiscoveryMinimumRecencyDays()) >= ttl)
        {
            throw new IllegalArgumentException("JobDiscoveryMinimumRecencyDays (in seconds) cannot be greater than "
                                               + ttl);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "job_discovery_active_loop_delay")
    public MillisecondBoundConfiguration jobDiscoveryActiveLoopDelay()
    {
        return jobDiscoveryActiveLoopDelay;
    }

    @JsonProperty(value = "job_discovery_active_loop_delay")
    public void setJobDiscoveryActiveLoopDelay(MillisecondBoundConfiguration jobDiscoveryActiveLoopDelay)
    {
        this.jobDiscoveryActiveLoopDelay = jobDiscoveryActiveLoopDelay;
    }

    /**
     * Legacy property {@code job_discovery_active_loop_delay_millis}
     *
     * @param jobDiscoveryActiveLoopDelayMillis active delay in milliseconds
     * @deprecated in favor of {@code job_discovery_active_loop_delay}
     */
    @JsonProperty(value = "job_discovery_active_loop_delay_millis")
    @Deprecated
    public void setJobDiscoveryActiveLoopDelayMillis(long jobDiscoveryActiveLoopDelayMillis)
    {
        LOGGER.warn("'job_discovery_active_loop_delay_millis' is deprecated, use 'job_discovery_active_loop_delay' instead");
        setJobDiscoveryActiveLoopDelay(new MillisecondBoundConfiguration(jobDiscoveryActiveLoopDelayMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "job_discovery_idle_loop_delay")
    public MillisecondBoundConfiguration jobDiscoveryIdleLoopDelay()
    {
        return jobDiscoveryIdleLoopDelay;
    }

    @JsonProperty(value = "job_discovery_idle_loop_delay")
    public void setJobDiscoveryIdleLoopDelay(MillisecondBoundConfiguration jobDiscoveryIdleLoopDelay)
    {
        this.jobDiscoveryIdleLoopDelay = jobDiscoveryIdleLoopDelay;
    }

    /**
     * Legacy property {@code job_discovery_idle_loop_delay_millis}
     *
     * @param jobDiscoveryIdleLoopDelayMillis idle delay in milliseconds
     * @deprecated in favor of {@code job_discovery_idle_loop_delay}
     */
    @JsonProperty(value = "job_discovery_idle_loop_delay_millis")
    @Deprecated
    public void setJobDiscoveryIdleLoopDelayMillis(long jobDiscoveryIdleLoopDelayMillis)
    {
        LOGGER.warn("'job_discovery_idle_loop_delay_millis' is deprecated, use 'job_discovery_idle_loop_delay' instead");
        setJobDiscoveryIdleLoopDelay(new MillisecondBoundConfiguration(jobDiscoveryIdleLoopDelayMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "job_discovery_minimum_recency_days")
    public int jobDiscoveryMinimumRecencyDays()
    {
        return jobDiscoveryMinimumRecencyDays;
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
    @JsonProperty(value = "restore_job_tables_ttl")
    public SecondBoundConfiguration restoreJobTablesTtl()
    {
        return restoreJobTablesTtl;
    }

    @JsonProperty(value = "restore_job_tables_ttl")
    public void setRestoreJobTablesTtl(SecondBoundConfiguration restoreJobTablesTtl)
    {
        this.restoreJobTablesTtl = restoreJobTablesTtl;
    }

    /**
     * Legacy property {@code restore_job_tables_ttl_seconds}
     *
     * @param restoreJobTablesTtlSeconds time-to-live in seconds
     * @deprecated in favor of {@code restore_job_tables_ttl}
     */
    @JsonProperty(value = "restore_job_tables_ttl_seconds")
    @Deprecated
    public void setRestoreJobTablesTtlSeconds(long restoreJobTablesTtlSeconds)
    {
        LOGGER.warn("'restore_job_tables_ttl_seconds' is deprecated, use 'restore_job_tables_ttl' instead");
        setRestoreJobTablesTtl(new SecondBoundConfiguration(restoreJobTablesTtlSeconds, TimeUnit.SECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "slow_task_threshold")
    public SecondBoundConfiguration slowTaskThreshold()
    {
        return slowTaskThreshold;
    }

    @JsonProperty(value = "slow_task_threshold")
    public void setSlowTaskThreshold(SecondBoundConfiguration slowTaskThreshold)
    {
        this.slowTaskThreshold = slowTaskThreshold;
    }

    /**
     * Legacy property {@code slow_task_threshold_seconds}
     *
     * @param slowTaskThresholdSeconds threshold in seconds
     * @deprecated in favor of {@code slow_task_threshold}
     */
    @JsonProperty(value = "slow_task_threshold_seconds")
    @Deprecated
    public void setSlowTaskThresholdSeconds(long slowTaskThresholdSeconds)
    {
        LOGGER.warn("'slow_task_threshold_seconds' is deprecated, use 'slow_task_threshold' instead");
        setSlowTaskThreshold(new SecondBoundConfiguration(slowTaskThresholdSeconds, TimeUnit.SECONDS));
    }

    @Override
    @JsonProperty(value = "slow_task_report_delay")
    public SecondBoundConfiguration slowTaskReportDelay()
    {
        return slowTaskReportDelay;
    }

    @JsonProperty(value = "slow_task_report_delay")
    public void setSlowTaskReportDelay(SecondBoundConfiguration slowTaskReportDelay)
    {
        this.slowTaskReportDelay = slowTaskReportDelay;
    }

    /**
     * Legacy property {@code slow_task_report_delay_seconds}
     *
     * @param slowTaskReportDelaySeconds delay in seconds
     * @deprecated in favor of {@code slow_task_report_delay}
     */
    @JsonProperty(value = "slow_task_report_delay_seconds")
    @Deprecated
    public void setSlowTaskReportDelaySeconds(long slowTaskReportDelaySeconds)
    {
        LOGGER.warn("'slow_task_report_delay_seconds' is deprecated, use 'slow_task_report_delay' instead");
        setSlowTaskReportDelay(new SecondBoundConfiguration(slowTaskReportDelaySeconds, TimeUnit.SECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "ring_topology_refresh_delay")
    public MillisecondBoundConfiguration ringTopologyRefreshDelay()
    {
        return ringTopologyRefreshDelay;
    }

    @JsonProperty(value = "ring_topology_refresh_delay")
    public void setRingTopologyRefreshDelay(MillisecondBoundConfiguration ringTopologyRefreshDelay)
    {
        this.ringTopologyRefreshDelay = ringTopologyRefreshDelay;
    }

    /**
     * Legacy property {@code ring_topology_refresh_delay_millis}
     *
     * @param ringTopologyRefreshDelayMillis refresh delay in milliseconds
     * @deprecated in favor of {@code ring_topology_refresh_delay}
     */
    @JsonProperty(value = "ring_topology_refresh_delay_millis")
    @Deprecated
    public void setRingTopologyRefreshDelayMillis(long ringTopologyRefreshDelayMillis)
    {
        LOGGER.warn("'ring_topology_refresh_delay_millis' is deprecated, use 'ring_topology_refresh_delay' instead");
        setRingTopologyRefreshDelay(new MillisecondBoundConfiguration(ringTopologyRefreshDelayMillis, TimeUnit.MILLISECONDS));
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
        private SecondBoundConfiguration slowTaskThreshold = DEFAULT_RESTORE_JOB_SLOW_TASK_THRESHOLD;
        private SecondBoundConfiguration slowTaskReportDelay = DEFAULT_RESTORE_JOB_SLOW_TASK_REPORT_DELAY;
        private MillisecondBoundConfiguration jobDiscoveryActiveLoopDelay = DEFAULT_JOB_DISCOVERY_ACTIVE_LOOP_DELAY;
        private MillisecondBoundConfiguration jobDiscoveryIdleLoopDelay = DEFAULT_JOB_DISCOVERY_IDLE_LOOP_DELAY;
        private int jobDiscoveryMinimumRecencyDays = DEFAULT_JOB_DISCOVERY_MINIMUM_RECENCY_DAYS;
        private int processMaxConcurrency = DEFAULT_PROCESS_MAX_CONCURRENCY;
        private SecondBoundConfiguration restoreJobTablesTtl = DEFAULT_RESTORE_JOB_TABLES_TTL;
        private MillisecondBoundConfiguration ringTopologyRefreshDelay = DEFAULT_RING_TOPOLOGY_REFRESH_DELAY;

        protected Builder()
        {
        }

        @Override
        public RestoreJobConfigurationImpl.Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code jobDiscoveryActiveLoopDelay} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param jobDiscoveryActiveLoopDelay the {@code jobDiscoveryActiveLoopDelay} to set
         * @return a reference to this Builder
         */
        public Builder jobDiscoveryActiveLoopDelay(MillisecondBoundConfiguration jobDiscoveryActiveLoopDelay)
        {
            return update(b -> b.jobDiscoveryActiveLoopDelay = jobDiscoveryActiveLoopDelay);
        }

        /**
         * Sets the {@code jobDiscoveryIdleLoopDelay} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param jobDiscoveryIdleLoopDelay the {@code jobDiscoveryIdleLoopDelay} to set
         * @return a reference to this Builder
         */
        public Builder jobDiscoveryIdleLoopDelay(MillisecondBoundConfiguration jobDiscoveryIdleLoopDelay)
        {
            return update(b -> b.jobDiscoveryIdleLoopDelay = jobDiscoveryIdleLoopDelay);
        }

        /**
         * Sets the {@code jobDiscoveryMinimumRecencyDays} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param jobDiscoveryMinimumRecencyDays the {@code jobDiscoveryMinimumRecencyDays} to set
         * @return a reference to this Builder
         */
        public Builder jobDiscoveryMinimumRecencyDays(int jobDiscoveryMinimumRecencyDays)
        {
            return update(b -> b.jobDiscoveryMinimumRecencyDays = jobDiscoveryMinimumRecencyDays);
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
         * Sets the {@code restoreJobTablesTtl} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param restoreJobTablesTtl the {@code restoreJobTablesTtl} to set
         * @return a reference to this Builder
         */
        public Builder restoreJobTablesTtl(SecondBoundConfiguration restoreJobTablesTtl)
        {
            return update(b -> b.restoreJobTablesTtl = restoreJobTablesTtl);
        }

        /**
         * Sets the {@code slowTaskThreshold} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param slowTaskThreshold the {@code slowTaskThreshold} to set
         * @return a reference to this Builder
         */
        public Builder slowTaskThreshold(SecondBoundConfiguration slowTaskThreshold)
        {
            return update(b -> b.slowTaskThreshold = slowTaskThreshold);
        }

        /**
         * Sets the {@code slowTaskReportDelay} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param slowTaskReportDelay the {@code slowTaskReportDelay} to set
         * @return a reference to this Builder
         */
        public Builder slowTaskReportDelay(SecondBoundConfiguration slowTaskReportDelay)
        {
            return update(b -> b.slowTaskReportDelay = slowTaskReportDelay);
        }

        /**
         * Sets the {@code ringTopologyRefreshDelay} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param ringTopologyRefreshDelay the {@code ringTopologyRefreshDelay} to set
         * @return a reference to this Builder
         */
        public Builder ringTopologyRefreshDelay(MillisecondBoundConfiguration ringTopologyRefreshDelay)
        {
            return update(b -> b.ringTopologyRefreshDelay = ringTopologyRefreshDelay);
        }

        @Override
        public RestoreJobConfigurationImpl build()
        {
            return new RestoreJobConfigurationImpl(this);
        }
    }
}
