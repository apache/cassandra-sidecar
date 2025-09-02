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

package org.apache.cassandra.sidecar.common.server.utils;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.CompactionInfo;
import org.apache.cassandra.sidecar.common.server.data.ActiveCompactionEntryData;
import org.apache.cassandra.sidecar.common.server.data.CompactionStatsData;

/**
 * Converter class for converting between data objects and API response objects for compaction stats
 */
public final class CompactionStatsConverter
{
    /**
     * Private constructor to prevent instantiation
     */
    private CompactionStatsConverter()
    {
        // utility class
    }

    /**
     * Converts CompactionStatsData to CompactionStatsResponse
     *
     * @param data the data object to convert
     * @return the API response object
     */
    public static CompactionStatsResponse toResponse(CompactionStatsData data)
    {
        if (data == null)
        {
            throw new IllegalStateException("No compaction stats data available");
        }

        CompactionStatsResponse.CompletedCompactionsRate responseRate = null;
        if (data.completedCompactionsRate != null)
        {
            responseRate = CompactionStatsResponse.CompletedCompactionsRate.builder()
                                                                           .meanRate(data.completedCompactionsRate.meanRate)
                                                                           .fifteenMinuteRate(data.completedCompactionsRate.fifteenMinuteRate)
                                                                           .build();
        }

        List<CompactionInfo> responseActiveCompactions = null;
        if (data.activeCompactions != null)
        {
            responseActiveCompactions = data.activeCompactions.stream()
                                                              .map(CompactionStatsConverter::toActiveCompactionEntry)
                                                              .collect(Collectors.toList());
        }

        return CompactionStatsResponse.builder()
                                      .concurrentCompactors(data.concurrentCompactors)
                                      .pendingTasks(data.pendingTasks)
                                      .totalPendingTasks(data.totalPendingTasks)
                                      .completedCompactions(data.completedCompactions)
                                      .dataCompacted(data.dataCompacted)
                                      .abortedCompactions(data.abortedCompactions)
                                      .reducedCompactions(data.reducedCompactions)
                                      .sstablesDroppedFromCompaction(data.sstablesDroppedFromCompaction)
                                      .completedCompactionsRate(responseRate)
                                      .activeCompactions(responseActiveCompactions)
                                      .activeCompactionsCount(data.activeCompactionsCount)
                                      .activeCompactionsRemainingTime(data.activeCompactionsRemainingTime)
                                      .build();
    }

    /**
     * Converts ActiveCompactionEntryData to ActiveCompactionEntry
     *
     * @param data the data object to convert
     * @return the API response object
     */
    private static CompactionInfo toActiveCompactionEntry(ActiveCompactionEntryData data)
    {
        if (data == null)
        {
            return null;
        }

        return CompactionInfo.builder()
                             .id(data.id)
                             .keyspace(data.keyspace)
                             .table(data.table)
                             .taskType(data.taskType)
                             .completedBytes(data.completedBytes)
                             .totalBytes(data.totalBytes)
                             .percentCompleted(data.percentCompleted)
                             .sstables(data.sstables)
                             .targetDirectory(data.targetDirectory)
                             .build();
    }
}
