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

package org.apache.cassandra.sidecar.adapters.base;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.openmbean.CompositeData;

import org.apache.cassandra.sidecar.adapters.base.jmx.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.sidecar.adapters.base.data.CompositeDataUtil.safeCast;
import static org.apache.cassandra.sidecar.adapters.base.data.CompositeDataUtil.safeParseLong;

import org.apache.cassandra.sidecar.adapters.base.data.SessionInfo;
import org.apache.cassandra.sidecar.adapters.base.data.StreamState;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStats;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsDatabaseAccessor;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsSummary;
import org.apache.cassandra.sidecar.common.response.CompactionStatsResponse;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ActiveCompactionEntry;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.common.response.data.StreamsProgressStats;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations.COMPACTION_MANAGER_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.jmx.StreamManagerJmxOperations.STREAM_MANAGER_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;

/**
 * Default implementation that pulls methods from the Cassandra Metrics Proxy
 */
public class CassandraMetricsOperations implements MetricsOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetricsOperations.class);
    private final ConnectedClientStatsDatabaseAccessor dbAccessor;
    protected final JmxClient jmxClient;

    private static final String METRICS_OBJ_TYPE_KEYSPACE_TABLE_FORMAT = "org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=%s";
    private static final String METRICS_OBJ_TYPE_COMPACTION = "org.apache.cassandra.metrics:type=Compaction,name=%s";

    // Constants for compaction info map keys
    public static final String ID = "id";
    public static final String KEYSPACE = "keyspace";
    public static final String COLUMNFAMILY = "columnfamily";
    public static final String COMPLETED = "completed";
    public static final String TOTAL = "total";
    public static final String TASK_TYPE = "taskType";
    public static final String COMPACTION_ID = "compactionId";
    public static final String SSTABLES = "sstables";
    public static final String TARGET_DIRECTORY = "targetDirectory";

    public static final String TIME_FORMAT = "%dh%02dm%02ds";

    // Default values
    public static final String DEFAULTVAL_STRING = "unknown";
    public static final String DEFAULTVAL_NUMBER = "-1";
    public static final String DEFAULTVAL_N_A = "n/a";

    /**
     * Creates a new instance with the provided {@link CQLSessionProvider}
     */
    public CassandraMetricsOperations(JmxClient jmxClient, TableSchemaFetcher tableSchemaFetcher, ICassandraAdapter cassandraAdapter)
    {
        this.jmxClient = jmxClient;
        this.dbAccessor = new ConnectedClientStatsDatabaseAccessor(tableSchemaFetcher, cassandraAdapter);
    }

    /**
     * Represents the types of metrics that are queried
     */
    public enum MetricType
    {
        GAUGE,
        COUNTER
    }

    /**
     * Represents the metrics related to table stats that are supported by the Sidecar
     */
    public enum TableStatsMetrics
    {
        SSTABLE_COUNT("LiveSSTableCount", MetricType.GAUGE),
        DISKSPACE_USED("LiveDiskSpaceUsed", MetricType.COUNTER),
        TOTAL_DISKSPACE_USED("TotalDiskSpaceUsed", MetricType.COUNTER),
        SNAPSHOTS_SIZE("SnapshotsSize", MetricType.GAUGE);

        private final String metricName;
        private final MetricType type;

        TableStatsMetrics(String metricName, MetricType type)
        {
            this.metricName = metricName;
            this.type = type;
        }

        String metricName()
        {
            return metricName;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableStatsResponse tableStats(QualifiedTableName tableName)
    {
        long sstableCount = queryTableMetric(tableName, TableStatsMetrics.SSTABLE_COUNT);
        long diskSpaceUsed = queryTableMetric(tableName, TableStatsMetrics.DISKSPACE_USED);
        long totalDiskSpaceUsed = queryTableMetric(tableName, TableStatsMetrics.TOTAL_DISKSPACE_USED);
        long snapshotsSize = queryTableMetric(tableName, TableStatsMetrics.SNAPSHOTS_SIZE);

        return new TableStatsResponse(tableName.keyspace(), tableName.tableName(), sstableCount, diskSpaceUsed, totalDiskSpaceUsed, snapshotsSize);
    }

    private long queryTableMetric(QualifiedTableName tableName, TableStatsMetrics metric)
    {
        String metricObjectType = String.format(METRICS_OBJ_TYPE_KEYSPACE_TABLE_FORMAT, tableName.keyspace(), tableName.tableName(), metric.metricName());
        return getValueAsLong(queryMetric(metricObjectType, metric.type));
    }

    private Object queryMetric(String metricObjectType, MetricType type)
    {
        switch(type)
        {
            case GAUGE:
                return jmxClient.proxy(GaugeMetricsJmxOperations.class, metricObjectType).getValue();
            case COUNTER:
                return jmxClient.proxy(CounterMetricsJmxOperations.class, metricObjectType).getCount();
            default:
                throw new IllegalArgumentException("Unknown MetricType: " + type);
        }
    }

    private long getValueAsLong(Object value)
    {
        if (value instanceof Integer)
        {
            return ((Integer) value).longValue();
        }
        else if (value instanceof Long)
        {
            return (Long) value;
        }
        else
        {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectedClientStatsResponse connectedClientStats(boolean summaryOnly)
    {
        if (summaryOnly)
        {
            return connectedClientSummary();
        }
        return connectedClientDetails();
    }

    public ConnectedClientStatsResponse connectedClientDetails()
    {
        List<ClientConnectionEntry> entries = statsToEntries(dbAccessor.stats());
        Map<String, Long> connectionsByUser = entries.stream().collect(Collectors.groupingBy(ClientConnectionEntry::username,
                                                                                             Collectors.counting()));
        long totalConnectedClients = entries.size();
        return new ConnectedClientStatsResponse(entries, totalConnectedClients, connectionsByUser);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamsProgressStats streamsProgressStats()
    {
        Set<CompositeData> streamData = jmxClient.proxy(StreamManagerJmxOperations.class, STREAM_MANAGER_OBJ_NAME)
                                                 .getCurrentStreams();
        return computeStats(streamData.stream().map(StreamState::new));
    }

    private StreamsProgressStats computeStats(Stream<StreamState> streamStates)
    {
        Iterator<SessionInfo> sessions = streamStates.map(StreamState::sessions).flatMap(Collection::stream).iterator();

        long totalFilesToReceive = 0;
        long totalFilesReceived = 0;
        long totalBytesToReceive = 0;
        long totalBytesReceived = 0;

        long totalFilesToSend = 0;
        long totalFilesSent = 0;
        long totalBytesToSend = 0;
        long totalBytesSent = 0;

        while (sessions.hasNext())
        {
            SessionInfo sessionInfo = sessions.next();
            totalBytesToReceive += sessionInfo.totalSizeToReceive();
            totalBytesReceived += sessionInfo.totalSizeReceived();
            totalFilesToReceive += sessionInfo.totalFilesToReceive();
            totalFilesReceived += sessionInfo.totalFilesReceived();
            totalBytesToSend += sessionInfo.totalSizeToSend();
            totalBytesSent += sessionInfo.totalSizeSent();
            totalFilesToSend += sessionInfo.totalFilesToSend();
            totalFilesSent += sessionInfo.totalFilesSent();
        }

        LOGGER.debug("Progress Stats: totalBytesToReceive:{} totalBytesReceived:{} totalBytesToSend:{} totalBytesSent:{}",
                     totalBytesToReceive, totalBytesReceived, totalBytesToSend, totalBytesSent);
        return new StreamsProgressStats(totalFilesToReceive, totalFilesReceived, totalBytesToReceive, totalBytesReceived,
                                        totalFilesToSend, totalFilesSent, totalBytesToSend, totalBytesSent);

    }

    private ConnectedClientStatsResponse connectedClientSummary()
    {
        ConnectedClientStatsSummary summary = dbAccessor.summary();
        return new ConnectedClientStatsResponse(null, summary.totalConnectedClients, summary.connectionsByUser);
    }

    private List<ClientConnectionEntry> statsToEntries(Stream<ConnectedClientStats> stats)
    {
        return stats.map(CassandraMetricsOperations::statToEntry)
                    .collect(Collectors.toList());
    }

    private static @NotNull ClientConnectionEntry statToEntry(ConnectedClientStats stat)
    {
        // Note: We explicitly use constructor params based object creation instead of builder in order to optimize the
        // number of potential objects created for each row of the table queried, specifically since we know this can be large
        return new ClientConnectionEntry(stat.address,
                                         stat.port,
                                         stat.sslEnabled,
                                         stat.sslCipherSuite,
                                         stat.sslProtocol,
                                         stat.protocolVersion,
                                         stat.username,
                                         stat.requestCount,
                                         stat.driverName,
                                         stat.driverVersion,
                                         stat.keyspaceName,
                                         stat.clientOptions,
                                         stat.authenticationMode,
                                         stat.authenticationMetadata);
    }

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
        private final MetricType type;

        CompactionStatsMetrics(String metricName, MetricType type)
        {
            this.metricName = metricName;
            this.type = type;
        }

        String metricName()
        {
            return metricName;
        }
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public CompactionStatsResponse compactionStats()
    {
        // Get compaction manager and storage service proxies
        CompactionManagerJmxOperations compactionManager = jmxClient.proxy(CompactionManagerJmxOperations.class, COMPACTION_MANAGER_OBJ_NAME);
        StorageJmxOperations storageService = jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME);

        // Get concurrent compactors from StorageService as per specification
        long concurrentCompactors = storageService.getConcurrentCompactors();

        // Get pending tasks grouped by keyspace and table
        Map<String, Map<String, Integer>> pendingTasks = getPendingCompactionTasksByTable();
        long totalPendingTasks = pendingTasks.values().stream()
                                                   .mapToLong(tableMap -> tableMap.values().stream().mapToInt(Integer::intValue).sum())
                                                   .sum();

        // Get compaction metrics from JMX counters and meters
        long completedCompactions = getValueAsLong(getCompactionMetric(CompactionStatsMetrics.TOTAL_COMPACTIONS_COMPLETED));
        long dataCompacted = getValueAsLong(getCompactionMetric(CompactionStatsMetrics.BYTES_COMPACTED));
        long abortedCompactions = getValueAsLong(getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_ABORTED));
        long reducedCompactions = getValueAsLong(getCompactionMetric(CompactionStatsMetrics.COMPACTIONS_REDUCED));
        long sstablesDroppedFromCompaction = getValueAsLong(getCompactionMetric(CompactionStatsMetrics.SSTABLES_DROPPED_FROM_COMPACTION));

        // Get completed compactions rate with proper time conversions
        CompactionStatsResponse.CompletedCompactionsRate completedCompactionsRate = getCompletedCompactionsRate();

        // Get active compactions with all required fields
        List<ActiveCompactionEntry> activeCompactions = getActiveCompactions(compactionManager.getCompactions());
        long activeCompactionsCount = activeCompactions.size();
        
        // Calculate remaining time in seconds based on throughput and remaining bytes
        String activeCompactionsRemainingTime = calculateRemainingTimeSeconds(activeCompactions, storageService);

        return new CompactionStatsResponse(concurrentCompactors, pendingTasks, totalPendingTasks,
                                           completedCompactions, dataCompacted, abortedCompactions,
                                           reducedCompactions, sstablesDroppedFromCompaction,
                                           completedCompactionsRate,
                                           activeCompactions, activeCompactionsCount, activeCompactionsRemainingTime);
    }

    private Map<String, Map<String, Integer>> getPendingCompactionTasksByTable()
    {
        // Get pending tasks by table name from Gauge metric
        Object value = getCompactionMetric(CompactionStatsMetrics.PENDING_TASKS_BY_TABLE_NAME);
        return parsePendingTasksMap(value);
    }

    private Map<String, Map<String, Integer>> parsePendingTasksMap(final Object value)
    {
        Map<?, ?> rawMap = safeCast(value, Map.class, "pending tasks");
        Map<String, Map<String, Integer>> result = new HashMap<>();

        for (Map.Entry<?, ?> entry : rawMap.entrySet())
        {
            String keyspace = safeCast(entry.getKey(), String.class, "keyspace name");
            Map<?, ?> rawTableMap = safeCast(entry.getValue(), Map.class, "table data");
            Map<String, Integer> tableMap = new HashMap<>();
            
            for (Map.Entry<?, ?> tableEntry : rawTableMap.entrySet())
            {
                String tableName = safeCast(tableEntry.getKey(), String.class, "table name");
                Number taskCount = safeCast(tableEntry.getValue(), Number.class, "task count");
                tableMap.put(tableName, taskCount.intValue());
            }
            
            result.put(keyspace, tableMap);
        }
        
        return result;
    }

    private Object getCompactionMetric(final CompactionStatsMetrics metric)
    {
        String metricObjectType = String.format(METRICS_OBJ_TYPE_COMPACTION, metric.metricName());
        return queryMetric(metricObjectType, metric.type);
    }

    private CompactionStatsResponse.CompletedCompactionsRate getCompletedCompactionsRate()
    {
        // Get rates from meter metric for TotalCompactionsCompleted
        String metricObjectType = String.format(METRICS_OBJ_TYPE_COMPACTION, "TotalCompactionsCompleted");
        MeterMetricsJmxOperations metricsProxy = jmxClient.proxy(MeterMetricsJmxOperations.class, metricObjectType);

        // Convert rates according to specification:
        // meanRate: compactions per hour
        // fifteenMinuteRate: compactions per minute for last 15 minutes
        double meanRateValue = metricsProxy.getMeanRate() * 3600; // Convert per second to per hour
        double fifteenMinuteRateValue = metricsProxy.getFifteenMinuteRate() * 60; // Convert per second to per minute

        String meanRate = String.format("%.2f/hour", meanRateValue);
        String fifteenMinuteRate = String.format("%.2f/minute", fifteenMinuteRateValue);

        return new CompactionStatsResponse.CompletedCompactionsRate(meanRate, fifteenMinuteRate);
    }


    private List<ActiveCompactionEntry> getActiveCompactions(final List<Map<String, String>> compactions)
    {
        return compactions.stream().map(compactionInfo -> {
            // Extract fields according to specification
            String id = compactionInfo.getOrDefault(COMPACTION_ID, DEFAULTVAL_STRING);
            String keyspace = compactionInfo.getOrDefault(KEYSPACE, DEFAULTVAL_STRING);
            String columnFamily = compactionInfo.getOrDefault(COLUMNFAMILY, DEFAULTVAL_STRING);
            String taskType = compactionInfo.getOrDefault(TASK_TYPE, DEFAULTVAL_STRING);
            
            // Parse byte values
            long completedBytes = safeParseLong(compactionInfo.getOrDefault(COMPLETED, DEFAULTVAL_NUMBER), "completed bytes");
            long totalBytes = safeParseLong(compactionInfo.getOrDefault(TOTAL, DEFAULTVAL_NUMBER), "total bytes");
            
            // Calculate percentage completed
            double percentCompleted = totalBytes == -1 ? 0.0 : (double) completedBytes / totalBytes * 100.0;
            
            // Parse SSTables list
            String ssTablesStr = compactionInfo.getOrDefault(SSTABLES, DEFAULTVAL_STRING);
            List<String> ssTables = ssTablesStr.isEmpty() ?
                List.of() : List.of(ssTablesStr.split(","));
            
            String targetDirectory = compactionInfo.getOrDefault(TARGET_DIRECTORY, DEFAULTVAL_STRING);
            
            return new ActiveCompactionEntry(id, keyspace, columnFamily, taskType, 
                                             completedBytes, totalBytes, percentCompleted, 
                                             ssTables, targetDirectory);
        }).collect(Collectors.toList());
    }

    private String calculateRemainingTimeSeconds(final List<ActiveCompactionEntry> activeCompactions,
                                               final StorageJmxOperations storageService)
    {
        if (activeCompactions.isEmpty())
        {
            return DEFAULTVAL_N_A;
        }

        // Calculate total remaining bytes across all active compactions
        long totalRemainingBytes = activeCompactions.stream()
                .filter(compaction -> compaction.totalBytes() >= 0 && compaction.completedBytes() >= 0)
                .mapToLong(compaction -> Math.max(0, compaction.totalBytes() - compaction.completedBytes()))
                .sum();

        long throughputBytesPerSec = storageService.getCompactionThroughtputBytesPerSec();
        if (totalRemainingBytes < 0 || throughputBytesPerSec <= 0)
        {
            return DEFAULTVAL_N_A;
        }

        // Calculate time in seconds (throughput is already in bytes/sec)
        long remainingTimeInSecs = totalRemainingBytes / throughputBytesPerSec;

        return String.format(TIME_FORMAT,
                remainingTimeInSecs / 3600,
                (remainingTimeInSecs % 3600) / 60,
                (remainingTimeInSecs % 60));
    }
}
