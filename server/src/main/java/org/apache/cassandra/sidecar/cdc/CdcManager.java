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

package org.apache.cassandra.sidecar.cdc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.ReplicationFactorSupplier;
import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.sidecar.SidecarCdcStats;
import org.apache.cassandra.cdc.sidecar.SidecarStatePersister;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;


/**
 * Manages the lifecycle and coordination of CDC (Change Data Capture) consumers for processing
 * Cassandra change events across distributed token ranges.
 *
 * <p>This class is responsible for:
 * <ul>
 *   <li>Building and configuring {@link SidecarCdc} consumers based on owned token ranges</li>
 *   <li>Deduplicating consumers by instance ID and token range to prevent duplicate processing</li>
 *   <li>Managing consumer lifecycle (start/stop operations)</li>
 *   <li>Integrating with various providers for cluster configuration, schema, and instance metadata</li>
 *   <li>Coordinating with the range manager to determine token ownership</li>
 * </ul>
 *
 * <p>The CDC consumers created by this manager process change events from Cassandra commit logs
 * and forward them to configured event consumers (such as Kafka producers) while maintaining
 * state persistence and proper token range distribution across the cluster.
 *
 * @see SidecarCdc
 * @see EventConsumer
 * @see RangeManager
 * @see CdcConfig
 */
public class CdcManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcManager.class);
    private static final int UNKNOWN_INSTANCE = -1;
    private final CdcConfig conf;
    private final RangeManager rangeManager;
    private final InstanceMetadataFetcher instanceFetcher;
    private final EventConsumer eventConsumer;
    private final SchemaSupplier schemaSupplier;
    private final ClusterConfigProvider clusterConfigProvider;
    private final SidecarCdcClient sidecarCdcClient;
    private final ICdcStats cdcStats;
    private List<CdcConsumerEntry> entries = new ArrayList<>();
    private final ReplicationFactorSupplier rfSupplier;
    private final CdcOptions cdcOptions;
    private final AsyncExecutor asyncExecutor;
    private final StateSidecarCdcCassandraClient cassandraClient;


    public CdcManager(EventConsumer eventConsumer,
                      SchemaSupplier schemaSupplier,
                      CdcConfig conf,
                      RangeManager rangeManager,
                      InstanceMetadataFetcher instanceFetcher,
                      ClusterConfigProvider clusterConfigProvider,
                      SidecarCdcClient sidecarCdcClient,
                      ICdcStats cdcStats,
                      TaskExecutorPool taskExecutorPool,
                      CdcDatabaseAccessor cdcDatabaseAccessor,
                      CdcOptions cdcOptions)
    {
        this.eventConsumer = eventConsumer;
        this.schemaSupplier = schemaSupplier;
        this.conf = conf;
        this.rangeManager = rangeManager;
        this.instanceFetcher = instanceFetcher;
        this.clusterConfigProvider = clusterConfigProvider;
        this.sidecarCdcClient = sidecarCdcClient;
        this.cdcStats = cdcStats;
        this.cdcOptions = cdcOptions;
        this.asyncExecutor = new ExecutorPoolsExecutor(taskExecutorPool);
        this.cassandraClient = new StateSidecarCdcCassandraClient(cdcDatabaseAccessor);
        this.rfSupplier = new SidecarReplicationFactorSupplier(cdcOptions, schemaSupplier);
    }

    List<CdcConsumerEntry> buildCdcConsumers()
    {
        Map<String, Set<TokenRange>> ownedRanges = rangeManager.ownedTokenRanges();
        if (ownedRanges == null || ownedRanges.isEmpty())
        {
            throw new IllegalStateException("No owned token ranges right now, cql session may still be initializing.");
        }

        // Deduplicate by (instanceId, tokenRange) to prevent duplicate consumers
        Map<String, CdcConsumerEntry> uniqueCdcConsumers = new HashMap<>(ownedRanges.values().stream().mapToInt(Set::size).sum());

        try
        {
            for (Map.Entry<String, Set<TokenRange>> entry : ownedRanges.entrySet())
            {
                for (TokenRange range : entry.getValue())
                {
                    Integer instanceId = getInstanceId(entry.getKey());

                    // Create unique key: "instanceId:rangeStart:rangeEnd"
                    String uniqueKey = String.format("%d:%s:%s",
                                                     instanceId,
                                                     range.startAsBigInt(),
                                                     range.endAsBigInt());

                    TokenRangeSupplier tokenRangeSupplier = () -> org.apache.cassandra.bridge.TokenRange.openClosed(range.startAsBigInt(),
                                                                                                                    range.endAsBigInt());
                    uniqueCdcConsumers.computeIfAbsent(uniqueKey,
                                                       k -> buildConsumer(conf.jobId(),
                                                                          instanceId,
                                                                          clusterConfigProvider,
                                                                          eventConsumer,
                                                                          schemaSupplier,
                                                                          tokenRangeSupplier,
                                                                          sidecarCdcClient,
                                                                          cdcStats));
                }
            }

            entries = new ArrayList<>(uniqueCdcConsumers.values());
            return entries;
        }
        catch (RuntimeException e)
        {
            // Stop any already-built consumers/persisters so timers and threads don't leak
            uniqueCdcConsumers.values().forEach(CdcConsumerEntry::stop);
            throw e;
        }
    }

    public void startConsumers()
    {
        entries.forEach(CdcConsumerEntry::start);
    }

    public void stopConsumers()
    {
        entries.forEach(CdcConsumerEntry::stop);
        entries.clear();
    }

    @VisibleForTesting
    Integer getInstanceId(String instanceIp)
    {
        try
        {
            return instanceFetcher.instance(instanceIp).id();
        }
        catch (Exception e)
        {
            LOGGER.warn("Requested IP {} does not match with any instances", instanceIp);
            return UNKNOWN_INSTANCE;
        }
    }


    CdcConsumerEntry buildConsumer(@NotNull String jobId,
                                   int partitionId,
                                   ClusterConfigProvider clusterConfigProvider,
                                   EventConsumer eventConsumer,
                                   SchemaSupplier schemaSupplier,
                                   TokenRangeSupplier tokenRangeSupplier,
                                   SidecarCdcClient sidecarCdcClient,
                                   ICdcStats cdcStats)
    {
        SidecarStatePersister persister = getSidecarStatePersister();
        SidecarCdc consumer = SidecarCdc.builder(jobId,
                                                  partitionId,
                                                  cdcOptions,
                                                  clusterConfigProvider,
                                                  eventConsumer,
                                                  schemaSupplier,
                                                  tokenRangeSupplier,
                                                  sidecarCdcClient,
                                                  cdcStats)
                                         .withExecutor(asyncExecutor)
                                         .withReplicationFactorSupplier(rfSupplier)
                                         .withSidecarStatePersister(persister)
                                         .build();
        return new CdcConsumerEntry(consumer, persister);
    }

    private @NotNull SidecarStatePersister getSidecarStatePersister()
    {
        return new SidecarStatePersister(org.apache.cassandra.cdc.sidecar.SidecarCdcOptions.DEFAULT,
                                         cdcOptions,
                                         SidecarCdcStats.STUB,
                                         cassandraClient,
                                         asyncExecutor);
    }
}
