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
import org.apache.cassandra.sidecar.common.response.data.ActiveCompactionEntry;

/**
 * Response class for the CompactionStats API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompactionStatsResponse
{
    private final long concurrentCompactors;
    private final Map<String, Map<String, Integer>> pendingTasks;
    private final long totalPendingTasks;
    private final long completedCompactions;
    private final long dataCompacted;
    private final long abortedCompactions;
    private final long reducedCompactions;
    private final long sstablesDroppedFromCompaction;
    private final CompletedCompactionsRate completedCompactionsRate;
    private final List<ActiveCompactionEntry> activeCompactions;
    private final long activeCompactionsCount;
    private final String activeCompactionsRemainingTime;

    /**
     * Represents the completed compactions rate
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CompletedCompactionsRate
    {
        private final String meanRate;
        private final String fifteenMinuteRate;

        @JsonCreator
        public CompletedCompactionsRate(@JsonProperty("meanRate") final String meanRate,
                                        @JsonProperty("fifteenMinuteRate") final String fifteenMinuteRate)
        {
            this.meanRate = meanRate;
            this.fifteenMinuteRate = fifteenMinuteRate;
        }

        @JsonProperty("meanRate")
        public String meanRate()
        {
            return meanRate;
        }

        @JsonProperty("fifteenMinuteRate")
        public String fifteenMinuteRate()
        {
            return fifteenMinuteRate;
        }
    }

    /**
     * Constructs a new {@link CompactionStatsResponse}.
     *
     * @param concurrentCompactors              number of concurrent compactors
     * @param pendingTasks                      pending compaction tasks by keyspace and table
     * @param totalPendingTasks                 total number of pending tasks
     * @param completedCompactions              total compactions completed
     * @param dataCompacted                     total data compacted in bytes
     * @param abortedCompactions                total compactions aborted
     * @param reducedCompactions                total compactions reduced
     * @param sstablesDroppedFromCompaction     total SSTables dropped from compaction
     * @param completedCompactionsRate          completed compactions rate statistics
     * @param activeCompactions                 list of active compactions
     * @param activeCompactionsCount            number of active compactions
     * @param activeCompactionsRemainingTime    estimated remaining time for active compactions formatted as "XhYYmZZs"
     */
    @JsonCreator
    public CompactionStatsResponse(@JsonProperty("concurrentCompactors") long concurrentCompactors,
                                   @JsonProperty("pendingTasks") Map<String, Map<String, Integer>> pendingTasks,
                                   @JsonProperty("totalPendingTasks") long totalPendingTasks,
                                   @JsonProperty("completedCompactions") long completedCompactions,
                                   @JsonProperty("dataCompacted") long dataCompacted,
                                   @JsonProperty("abortedCompactions") long abortedCompactions,
                                   @JsonProperty("reducedCompactions") long reducedCompactions,
                                   @JsonProperty("sstablesDroppedFromCompaction") long sstablesDroppedFromCompaction,
                                   @JsonProperty("completedCompactionsRate") CompletedCompactionsRate completedCompactionsRate,
                                   @JsonProperty("activeCompactions") List<ActiveCompactionEntry> activeCompactions,
                                   @JsonProperty("activeCompactionsCount") long activeCompactionsCount,
                                   @JsonProperty("activeCompactionsRemainingTime") String activeCompactionsRemainingTime)
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

    @JsonProperty("concurrentCompactors")
    public long concurrentCompactors()
    {
        return concurrentCompactors;
    }

    @JsonProperty("pendingTasks")
    public Map<String, Map<String, Integer>> pendingTasks()
    {
        return pendingTasks;
    }

    @JsonProperty("totalPendingTasks")
    public long totalPendingTasks()
    {
        return totalPendingTasks;
    }

    @JsonProperty("completedCompactions")
    public long completedCompactions()
    {
        return completedCompactions;
    }

    @JsonProperty("dataCompacted")
    public long dataCompacted()
    {
        return dataCompacted;
    }

    @JsonProperty("abortedCompactions")
    public long abortedCompactions()
    {
        return abortedCompactions;
    }

    @JsonProperty("reducedCompactions")
    public long reducedCompactions()
    {
        return reducedCompactions;
    }

    @JsonProperty("sstablesDroppedFromCompaction")
    public long sstablesDroppedFromCompaction()
    {
        return sstablesDroppedFromCompaction;
    }

    @JsonProperty("completedCompactionsRate")
    public CompletedCompactionsRate completedCompactionsRate()
    {
        return completedCompactionsRate;
    }


    @JsonProperty("activeCompactions")
    public List<ActiveCompactionEntry> activeCompactions()
    {
        return activeCompactions;
    }

    @JsonProperty("activeCompactionsCount")
    public long activeCompactionsCount()
    {
        return activeCompactionsCount;
    }

    @JsonProperty("activeCompactionsRemainingTime")
    public String activeCompactionsRemainingTime()
    {
        return activeCompactionsRemainingTime;
    }
}
