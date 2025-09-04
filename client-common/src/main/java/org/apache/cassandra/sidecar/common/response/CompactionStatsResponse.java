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

package org.apache.cassandra.sidecar.common.response;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.response.data.CompactionInfo;

/**
 * Response class for the CompactionStats API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompactionStatsResponse
{
    public static final String CONCURRENT_COMPACTORS = "concurrentCompactors";
    public static final String PENDING_TASKS = "pendingTasks";
    public static final String TOTAL_PENDING_TASKS = "totalPendingTasks";
    public static final String COMPLETED_COMPACTIONS = "completedCompactions";
    public static final String DATA_COMPACTED = "dataCompacted";
    public static final String ABORTED_COMPACTIONS = "abortedCompactions";
    public static final String REDUCED_COMPACTIONS = "reducedCompactions";
    public static final String SSTABLES_DROPPED_FROM_COMPACTION = "sstablesDroppedFromCompaction";
    public static final String COMPLETED_COMPACTIONS_RATE = "completedCompactionsRate";
    public static final String ACTIVE_COMPACTIONS = "activeCompactions";
    public static final String ACTIVE_COMPACTIONS_COUNT = "activeCompactionsCount";
    public static final String ACTIVE_COMPACTIONS_REMAINING_TIME = "activeCompactionsRemainingTime";

    private final long concurrentCompactors;
    private final Map<String, Map<String, Integer>> pendingTasks;
    private final long totalPendingTasks;
    private final long completedCompactions;
    private final long dataCompacted;
    private final long abortedCompactions;
    private final long reducedCompactions;
    private final long sstablesDroppedFromCompaction;
    private final CompletedCompactionsRate completedCompactionsRate;
    private final List<CompactionInfo> activeCompactions;
    private final long activeCompactionsCount;
    private final long activeCompactionsRemainingTime;

    /**
     * Represents the completed compactions rate
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CompletedCompactionsRate
    {
        public static final String MEAN_RATE = "meanRate";
        public static final String FIFTEEN_MINUTE_RATE = "fifteenMinuteRate";

        private final double meanRate;
        private final double fifteenMinuteRate;

        private CompletedCompactionsRate(Builder builder)
        {
            this.meanRate = builder.meanRate;
            this.fifteenMinuteRate = builder.fifteenMinuteRate;
        }

        @JsonCreator
        public CompletedCompactionsRate(@JsonProperty(MEAN_RATE) final double meanRate,
                                        @JsonProperty(FIFTEEN_MINUTE_RATE) final double fifteenMinuteRate)
        {
            this.meanRate = meanRate;
            this.fifteenMinuteRate = fifteenMinuteRate;
        }

        @JsonProperty(MEAN_RATE)
        public double meanRate()
        {
            return meanRate;
        }

        @JsonProperty(FIFTEEN_MINUTE_RATE)
        public double fifteenMinuteRate()
        {
            return fifteenMinuteRate;
        }

        @Override
        public String toString()
        {
            return String.format("CompletedCompactionsRate{" +
                                 "meanRate=%.2f, " +
                                 "fifteenMinuteRate=%.2f}",
                                 meanRate, fifteenMinuteRate);
        }

        public static Builder builder()
        {
            return new Builder();
        }

        /**
         * {@code CompletedCompactionsRate} builder static inner class.
         */
        public static final class Builder implements DataObjectBuilder<Builder, CompletedCompactionsRate>
        {
            private double meanRate;
            private double fifteenMinuteRate;

            private Builder()
            {
            }

            @Override
            public Builder self()
            {
                return this;
            }

            /**
             * Sets the {@code meanRate} and returns a reference to this Builder enabling method chaining.
             *
             * @param meanRate the {@code meanRate} to set
             * @return a reference to this Builder
             */
            public Builder meanRate(double meanRate)
            {
                return update(b -> b.meanRate = meanRate);
            }

            /**
             * Sets the {@code fifteenMinuteRate} and returns a reference to this Builder enabling method chaining.
             *
             * @param fifteenMinuteRate the {@code fifteenMinuteRate} to set
             * @return a reference to this Builder
             */
            public Builder fifteenMinuteRate(double fifteenMinuteRate)
            {
                return update(b -> b.fifteenMinuteRate = fifteenMinuteRate);
            }

            /**
             * Returns a {@code CompletedCompactionsRate} built from the parameters previously set.
             *
             * @return a {@code CompletedCompactionsRate} built with parameters of this {@code CompletedCompactionsRate.Builder}
             */
            @Override
            public CompletedCompactionsRate build()
            {
                return new CompletedCompactionsRate(this);
            }
        }
    }

    private CompactionStatsResponse(Builder builder)
    {
        this.concurrentCompactors = builder.concurrentCompactors;
        this.pendingTasks = builder.pendingTasks;
        this.totalPendingTasks = builder.totalPendingTasks;
        this.completedCompactions = builder.completedCompactions;
        this.dataCompacted = builder.dataCompacted;
        this.abortedCompactions = builder.abortedCompactions;
        this.reducedCompactions = builder.reducedCompactions;
        this.sstablesDroppedFromCompaction = builder.sstablesDroppedFromCompaction;
        this.completedCompactionsRate = builder.completedCompactionsRate;
        this.activeCompactions = builder.activeCompactions;
        this.activeCompactionsCount = builder.activeCompactionsCount;
        this.activeCompactionsRemainingTime = builder.activeCompactionsRemainingTime;
    }

    /**
     * Constructs a new {@link CompactionStatsResponse}.
     *
     * @param concurrentCompactors           number of concurrent compactors
     * @param pendingTasks                   pending compaction tasks by keyspace and table
     * @param totalPendingTasks              total number of pending tasks
     * @param completedCompactions           total compactions completed
     * @param dataCompacted                  total data compacted in bytes
     * @param abortedCompactions             total compactions aborted
     * @param reducedCompactions             total compactions reduced
     * @param sstablesDroppedFromCompaction  total SSTables dropped from compaction
     * @param completedCompactionsRate       completed compactions rate statistics
     * @param activeCompactions              list of active compactions
     * @param activeCompactionsCount         number of active compactions
     * @param activeCompactionsRemainingTime estimated remaining time for active compactions in seconds
     */
    @JsonCreator
    public CompactionStatsResponse(@JsonProperty(CONCURRENT_COMPACTORS) long concurrentCompactors,
                                   @JsonProperty(PENDING_TASKS) Map<String, Map<String, Integer>> pendingTasks,
                                   @JsonProperty(TOTAL_PENDING_TASKS) long totalPendingTasks,
                                   @JsonProperty(COMPLETED_COMPACTIONS) long completedCompactions,
                                   @JsonProperty(DATA_COMPACTED) long dataCompacted,
                                   @JsonProperty(ABORTED_COMPACTIONS) long abortedCompactions,
                                   @JsonProperty(REDUCED_COMPACTIONS) long reducedCompactions,
                                   @JsonProperty(SSTABLES_DROPPED_FROM_COMPACTION) long sstablesDroppedFromCompaction,
                                   @JsonProperty(COMPLETED_COMPACTIONS_RATE) CompletedCompactionsRate completedCompactionsRate,
                                   @JsonProperty(ACTIVE_COMPACTIONS) List<CompactionInfo> activeCompactions,
                                   @JsonProperty(ACTIVE_COMPACTIONS_COUNT) long activeCompactionsCount,
                                   @JsonProperty(ACTIVE_COMPACTIONS_REMAINING_TIME) long activeCompactionsRemainingTime)
    {
        this.concurrentCompactors = concurrentCompactors;
        this.pendingTasks = pendingTasks;
        this.totalPendingTasks = totalPendingTasks;
        this.completedCompactions = completedCompactions;
        this.dataCompacted = dataCompacted;
        this.abortedCompactions = abortedCompactions;
        this.reducedCompactions = reducedCompactions;
        this.sstablesDroppedFromCompaction = sstablesDroppedFromCompaction;
        this.completedCompactionsRate = completedCompactionsRate;
        this.activeCompactions = activeCompactions;
        this.activeCompactionsCount = activeCompactionsCount;
        this.activeCompactionsRemainingTime = activeCompactionsRemainingTime;
    }

    @JsonProperty(CONCURRENT_COMPACTORS)
    public long concurrentCompactors()
    {
        return concurrentCompactors;
    }

    @JsonProperty(PENDING_TASKS)
    public Map<String, Map<String, Integer>> pendingTasks()
    {
        return pendingTasks;
    }

    @JsonProperty(TOTAL_PENDING_TASKS)
    public long totalPendingTasks()
    {
        return totalPendingTasks;
    }

    @JsonProperty(COMPLETED_COMPACTIONS)
    public long completedCompactions()
    {
        return completedCompactions;
    }

    @JsonProperty(DATA_COMPACTED)
    public long dataCompacted()
    {
        return dataCompacted;
    }

    @JsonProperty(ABORTED_COMPACTIONS)
    public long abortedCompactions()
    {
        return abortedCompactions;
    }

    @JsonProperty(REDUCED_COMPACTIONS)
    public long reducedCompactions()
    {
        return reducedCompactions;
    }

    @JsonProperty(SSTABLES_DROPPED_FROM_COMPACTION)
    public long sstablesDroppedFromCompaction()
    {
        return sstablesDroppedFromCompaction;
    }

    @JsonProperty(COMPLETED_COMPACTIONS_RATE)
    public CompletedCompactionsRate completedCompactionsRate()
    {
        return completedCompactionsRate;
    }


    @JsonProperty(ACTIVE_COMPACTIONS)
    public List<CompactionInfo> activeCompactions()
    {
        return activeCompactions;
    }

    @JsonProperty(ACTIVE_COMPACTIONS_COUNT)
    public long activeCompactionsCount()
    {
        return activeCompactionsCount;
    }

    @JsonProperty(ACTIVE_COMPACTIONS_REMAINING_TIME)
    public long activeCompactionsRemainingTime()
    {
        return activeCompactionsRemainingTime;
    }

    @Override
    public String toString()
    {
        return String.format("CompactionStatsResponse{" +
                             "concurrentCompactors=%d, " +
                             "pendingTasks=%s, " +
                             "totalPendingTasks=%d, " +
                             "completedCompactions=%d, " +
                             "dataCompacted=%d, " +
                             "abortedCompactions=%d, " +
                             "reducedCompactions=%d, " +
                             "sstablesDroppedFromCompaction=%d, " +
                             "completedCompactionsRate=%s, " +
                             "activeCompactions=%s, " +
                             "activeCompactionsCount=%d, " +
                             "activeCompactionsRemainingTime=%d}",
                             concurrentCompactors, pendingTasks, totalPendingTasks, completedCompactions,
                             dataCompacted, abortedCompactions, reducedCompactions, sstablesDroppedFromCompaction,
                             completedCompactionsRate, activeCompactions, activeCompactionsCount, activeCompactionsRemainingTime);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code CompactionStatsResponse} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, CompactionStatsResponse>
    {
        private long concurrentCompactors;
        private Map<String, Map<String, Integer>> pendingTasks;
        private long totalPendingTasks;
        private long completedCompactions;
        private long dataCompacted;
        private long abortedCompactions;
        private long reducedCompactions;
        private long sstablesDroppedFromCompaction;
        private CompletedCompactionsRate completedCompactionsRate;
        private List<CompactionInfo> activeCompactions;
        private long activeCompactionsCount;
        private long activeCompactionsRemainingTime;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code concurrentCompactors} and returns a reference to this Builder enabling method chaining.
         *
         * @param concurrentCompactors the {@code concurrentCompactors} to set
         * @return a reference to this Builder
         */
        public Builder concurrentCompactors(long concurrentCompactors)
        {
            return update(b -> b.concurrentCompactors = concurrentCompactors);
        }

        /**
         * Sets the {@code pendingTasks} and returns a reference to this Builder enabling method chaining.
         *
         * @param pendingTasks the {@code pendingTasks} to set
         * @return a reference to this Builder
         */
        public Builder pendingTasks(Map<String, Map<String, Integer>> pendingTasks)
        {
            return update(b -> b.pendingTasks = pendingTasks);
        }

        /**
         * Sets the {@code totalPendingTasks} and returns a reference to this Builder enabling method chaining.
         *
         * @param totalPendingTasks the {@code totalPendingTasks} to set
         * @return a reference to this Builder
         */
        public Builder totalPendingTasks(long totalPendingTasks)
        {
            return update(b -> b.totalPendingTasks = totalPendingTasks);
        }

        /**
         * Sets the {@code completedCompactions} and returns a reference to this Builder enabling method chaining.
         *
         * @param completedCompactions the {@code completedCompactions} to set
         * @return a reference to this Builder
         */
        public Builder completedCompactions(long completedCompactions)
        {
            return update(b -> b.completedCompactions = completedCompactions);
        }

        /**
         * Sets the {@code dataCompacted} and returns a reference to this Builder enabling method chaining.
         *
         * @param dataCompacted the {@code dataCompacted} to set
         * @return a reference to this Builder
         */
        public Builder dataCompacted(long dataCompacted)
        {
            return update(b -> b.dataCompacted = dataCompacted);
        }

        /**
         * Sets the {@code abortedCompactions} and returns a reference to this Builder enabling method chaining.
         *
         * @param abortedCompactions the {@code abortedCompactions} to set
         * @return a reference to this Builder
         */
        public Builder abortedCompactions(long abortedCompactions)
        {
            return update(b -> b.abortedCompactions = abortedCompactions);
        }

        /**
         * Sets the {@code reducedCompactions} and returns a reference to this Builder enabling method chaining.
         *
         * @param reducedCompactions the {@code reducedCompactions} to set
         * @return a reference to this Builder
         */
        public Builder reducedCompactions(long reducedCompactions)
        {
            return update(b -> b.reducedCompactions = reducedCompactions);
        }

        /**
         * Sets the {@code sstablesDroppedFromCompaction} and returns a reference to this Builder enabling method chaining.
         *
         * @param sstablesDroppedFromCompaction the {@code sstablesDroppedFromCompaction} to set
         * @return a reference to this Builder
         */
        public Builder sstablesDroppedFromCompaction(long sstablesDroppedFromCompaction)
        {
            return update(b -> b.sstablesDroppedFromCompaction = sstablesDroppedFromCompaction);
        }

        /**
         * Sets the {@code completedCompactionsRate} and returns a reference to this Builder enabling method chaining.
         *
         * @param completedCompactionsRate the {@code completedCompactionsRate} to set
         * @return a reference to this Builder
         */
        public Builder completedCompactionsRate(CompletedCompactionsRate completedCompactionsRate)
        {
            return update(b -> b.completedCompactionsRate = completedCompactionsRate);
        }

        /**
         * Sets the {@code activeCompactions} and returns a reference to this Builder enabling method chaining.
         *
         * @param activeCompactions the {@code activeCompactions} to set
         * @return a reference to this Builder
         */
        public Builder activeCompactions(List<CompactionInfo> activeCompactions)
        {
            return update(b -> b.activeCompactions = activeCompactions);
        }

        /**
         * Sets the {@code activeCompactionsCount} and returns a reference to this Builder enabling method chaining.
         *
         * @param activeCompactionsCount the {@code activeCompactionsCount} to set
         * @return a reference to this Builder
         */
        public Builder activeCompactionsCount(long activeCompactionsCount)
        {
            return update(b -> b.activeCompactionsCount = activeCompactionsCount);
        }

        /**
         * Sets the {@code activeCompactionsRemainingTime} and returns a reference to this Builder enabling method chaining.
         *
         * @param activeCompactionsRemainingTime the {@code activeCompactionsRemainingTime} to set
         * @return a reference to this Builder
         */
        public Builder activeCompactionsRemainingTime(long activeCompactionsRemainingTime)
        {
            return update(b -> b.activeCompactionsRemainingTime = activeCompactionsRemainingTime);
        }

        /**
         * Returns a {@code CompactionStatsResponse} built from the parameters previously set.
         *
         * @return a {@code CompactionStatsResponse} built with parameters of this {@code CompactionStatsResponse.Builder}
         */
        @Override
        public CompactionStatsResponse build()
        {
            return new CompactionStatsResponse(this);
        }
    }
}
