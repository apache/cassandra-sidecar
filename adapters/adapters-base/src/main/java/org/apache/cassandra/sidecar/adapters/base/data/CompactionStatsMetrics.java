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

package org.apache.cassandra.sidecar.adapters.base.data;

import org.apache.cassandra.sidecar.common.server.data.MetricType;

/**
 * Represents the metrics related to compaction stats that are supported by the Sidecar
 */
public enum CompactionStatsMetrics
{
    TOTAL_COMPACTIONS_COMPLETED("TotalCompactionsCompleted", MetricType.COUNTER),
    BYTES_COMPACTED("BytesCompacted", MetricType.COUNTER),
    COMPACTIONS_ABORTED("CompactionsAborted", MetricType.COUNTER),
    COMPACTIONS_REDUCED("CompactionsReduced", MetricType.COUNTER),
    SSTABLES_DROPPED_FROM_COMPACTION("SSTablesDroppedFromCompaction", MetricType.COUNTER),
    PENDING_TASKS_BY_TABLE_NAME("PendingTasksByTableName", MetricType.GAUGE);

    private final String metricName;
    public final MetricType type;

    CompactionStatsMetrics(String metricName, MetricType type)
    {
        this.metricName = metricName;
        this.type = type;
    }

    public String metricName()
    {
        return metricName;
    }
}
