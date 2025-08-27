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

package org.apache.cassandra.sidecar.common.server.data;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Data object representing compaction statistics
 */
public class CompactionStatsData
{
    public final long concurrentCompactors;
    public final Map<String, Map<String, Integer>> pendingTasks;
    public final long totalPendingTasks;
    public final long completedCompactions;
    public final long dataCompacted;
    public final long abortedCompactions;
    public final long reducedCompactions;
    public final long sstablesDroppedFromCompaction;
    public final CompletedCompactionsRateData completedCompactionsRate;
    public final List<ActiveCompactionEntryData> activeCompactions;
    public final long activeCompactionsCount;
    public final long activeCompactionsRemainingTime;

    private CompactionStatsData(Builder builder)
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


    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code CompactionStatsData} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, CompactionStatsData>
    {
        private long concurrentCompactors;
        private Map<String, Map<String, Integer>> pendingTasks;
        private long totalPendingTasks;
        private long completedCompactions;
        private long dataCompacted;
        private long abortedCompactions;
        private long reducedCompactions;
        private long sstablesDroppedFromCompaction;
        private CompletedCompactionsRateData completedCompactionsRate;
        private List<ActiveCompactionEntryData> activeCompactions;
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
        public Builder completedCompactionsRate(CompletedCompactionsRateData completedCompactionsRate)
        {
            return update(b -> b.completedCompactionsRate = completedCompactionsRate);
        }

        /**
         * Sets the {@code activeCompactions} and returns a reference to this Builder enabling method chaining.
         *
         * @param activeCompactions the {@code activeCompactions} to set
         * @return a reference to this Builder
         */
        public Builder activeCompactions(List<ActiveCompactionEntryData> activeCompactions)
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
         * Returns a {@code CompactionStatsData} built from the parameters previously set.
         *
         * @return a {@code CompactionStatsData} built with parameters of this {@code CompactionStatsData.Builder}
         */
        @Override
        public CompactionStatsData build()
        {
            return new CompactionStatsData(this);
        }
    }
}
