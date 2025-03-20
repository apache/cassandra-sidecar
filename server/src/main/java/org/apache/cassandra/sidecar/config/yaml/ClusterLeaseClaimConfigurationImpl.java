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
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.ClusterLeaseClaimConfiguration;

/**
 * Configuration for the {@link org.apache.cassandra.sidecar.coordination.ClusterLeaseClaimTask}
 */
public class ClusterLeaseClaimConfigurationImpl extends PeriodicTaskConfigurationImpl implements ClusterLeaseClaimConfiguration
{
    private static final String DEFAULT_ELECTORATE_MEMBERSHIP_STRATEGY = "MostReplicatedKeyspaceTokenZeroElectorateMembership";
    private static final MillisecondBoundConfiguration DEFAULT_INITIAL_DELAY_RANDOM_DELTA = MillisecondBoundConfiguration.parse("30s");
    public static final PeriodicTaskConfigurationImpl.Builder DEFAULT_PERIODIC_TASK_BUILDER = PeriodicTaskConfigurationImpl.Builder
                                                                                              .builder()
                                                                                              .enabled(true)
                                                                                              .initialDelay(MillisecondBoundConfiguration.parse("1s"))
                                                                                              .executeInterval(MillisecondBoundConfiguration.parse("100s"));

    @JsonProperty("electorate_membership_strategy")
    private final String electorateMembershipStrategy;

    @JsonProperty("initial_delay_random_delta")
    private final MillisecondBoundConfiguration initialDelayRandomDelta;

    @JsonCreator
    public ClusterLeaseClaimConfigurationImpl()
    {
        super(DEFAULT_PERIODIC_TASK_BUILDER);
        this.electorateMembershipStrategy = DEFAULT_ELECTORATE_MEMBERSHIP_STRATEGY;
        this.initialDelayRandomDelta = DEFAULT_INITIAL_DELAY_RANDOM_DELTA;
    }

    private ClusterLeaseClaimConfigurationImpl(Builder builder)
    {
        super(builder.periodicTaskBuilder);
        electorateMembershipStrategy = Objects.requireNonNull(builder.electorateMembershipStrategy, "electorateMembershipStrategy is required");
        initialDelayRandomDelta = builder.initialDelayRandomDelta;
    }

    @Override
    @JsonProperty("electorate_membership_strategy")
    public String electorateMembershipStrategy()
    {
        return electorateMembershipStrategy;
    }

    @Override
    @JsonProperty("initial_delay_random_delta")
    public MillisecondBoundConfiguration initialDelayRandomDelta()
    {
        return initialDelayRandomDelta;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code ClusterLeaseClaimConfigurationImpl} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, ClusterLeaseClaimConfigurationImpl>
    {
        private String electorateMembershipStrategy = DEFAULT_ELECTORATE_MEMBERSHIP_STRATEGY;
        private MillisecondBoundConfiguration initialDelayRandomDelta = DEFAULT_INITIAL_DELAY_RANDOM_DELTA;
        private final PeriodicTaskConfigurationImpl.Builder periodicTaskBuilder = DEFAULT_PERIODIC_TASK_BUILDER;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code electorateMembershipStrategy} and returns a reference to this Builder enabling method chaining.
         *
         * @param electorateMembershipStrategy the {@code electorateMembershipStrategy} to set
         * @return a reference to this Builder
         */
        public Builder electorateMembershipStrategy(String electorateMembershipStrategy)
        {
            return update(b -> b.electorateMembershipStrategy = electorateMembershipStrategy);
        }

        /**
         * Sets the {@code initialDelayRandomDelta} and returns a reference to this Builder enabling method chaining.
         *
         * @param initialDelayRandomDelta the {@code initialDelayRandomDelta} to set
         * @return a reference to this Builder
         */
        public Builder initialDelayRandomDelta(MillisecondBoundConfiguration initialDelayRandomDelta)
        {
            return update(b -> b.initialDelayRandomDelta = initialDelayRandomDelta);
        }

        public Builder overridePeriodicTaskConfiguration(Consumer<PeriodicTaskConfigurationImpl.Builder> overrides)
        {
            periodicTaskBuilder.update(overrides);
            return self();
        }

        /**
         * Returns a {@code ClusterLeaseClaimConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code ClusterLeaseClaimConfigurationImpl} built with parameters of this {@code ClusterLeaseClaimConfigurationImpl.Builder}
         */
        @Override
        public ClusterLeaseClaimConfigurationImpl build()
        {
            return new ClusterLeaseClaimConfigurationImpl(this);
        }
    }
}
