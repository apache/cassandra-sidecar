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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.sidecar.SidecarCdcStats;
import org.apache.cassandra.cdc.sidecar.SidecarStatePersister;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.secrets.SecretsProvider;
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
    private final CdcSidecarInstancesProvider sidecarInstancesProvider;
    private final SecretsProvider secretsProvider;
    private final SidecarCdcClient.ClientConfig clientConfig;
    private final ICdcStats cdcStats;
    private List<SidecarCdc> consumers = new ArrayList<>();
    private final TaskExecutorPool taskExecutorPool;
    private final CdcDatabaseAccessor cdcDatabaseAccessor;


    public CdcManager(EventConsumer eventConsumer,
                      SchemaSupplier schemaSupplier,
                      CdcConfig conf,
                      RangeManager rangeManager,
                      InstanceMetadataFetcher instanceFetcher,
                      ClusterConfigProvider clusterConfigProvider,
                      CdcSidecarInstancesProvider sidecarInstancesProvider,
                      SecretsProvider secretsProvider,
                      SidecarCdcClient.ClientConfig clientConfig,
                      ICdcStats cdcStats,
                      TaskExecutorPool taskExecutorPool,
                      CdcDatabaseAccessor cdcDatabaseAccessor)
    {
        this.eventConsumer = eventConsumer;
        this.schemaSupplier = schemaSupplier;
        this.conf = conf;
        this.rangeManager = rangeManager;
        this.instanceFetcher = instanceFetcher;
        this.clusterConfigProvider = clusterConfigProvider;
        this.sidecarInstancesProvider = sidecarInstancesProvider;
        this.secretsProvider = secretsProvider;
        this.clientConfig = clientConfig;
        this.cdcStats = cdcStats;
        this.taskExecutorPool = taskExecutorPool;
        this.cdcDatabaseAccessor = cdcDatabaseAccessor;
    }

    List<SidecarCdc> buildCdcConsumers()
    {
        Map<String, Set<TokenRange>> ownedRanges = rangeManager.ownedTokenRanges();
        if (ownedRanges == null || ownedRanges.isEmpty())
        {
            throw new IllegalStateException("No owned token ranges right now, cql session may still be initializing.");
        }

        // NEW: Deduplicate by (instanceId, tokenRange) to prevent duplicate consumers
        Map<String, SidecarCdc> uniqueConsumers = new HashMap<>(ownedRanges.values().stream().mapToInt(Set::size).sum());

        ownedRanges.entrySet().stream()
                   .flatMap(entry ->
                            entry.getValue().stream().map(range -> {
                                Integer instanceId = getInstanceId(entry.getKey());

                                // Create unique key: "instanceId:rangeStart:rangeEnd"
                                String uniqueKey = String.format("%d:%s:%s",
                                                                 instanceId,
                                                                 range.startAsBigInt(),
                                                                 range.endAsBigInt());

                                // Only create consumer if not already created for this (instance, range)
                                return uniqueConsumers.computeIfAbsent(uniqueKey, k -> {
                                    try
                                    {
                                        return loadOrBuildCdcConsumer(instanceId,
                                                                      clusterConfigProvider,
                                                                      eventConsumer,
                                                                      schemaSupplier,
                                                                      () -> org.apache.cassandra.bridge.TokenRange.openClosed(range.startAsBigInt(), range.endAsBigInt()),
                                                                      sidecarInstancesProvider,
                                                                      secretsProvider,
                                                                      clientConfig,
                                                                      conf,
                                                                      cdcStats,
                                                                      taskExecutorPool);
                                    }
                                    catch (IOException e)
                                    {
                                        throw new RuntimeException(e);
                                    }
                                });
                            }))
                   .collect(Collectors.toList());

        consumers = new ArrayList<>(uniqueConsumers.values());
        return consumers;
    }

    SidecarCdc loadOrBuildCdcConsumer(Integer instanceId,
                                      ClusterConfigProvider clusterConfigProvider,
                                      EventConsumer eventConsumer,
                                      SchemaSupplier schemaSupplier,
                                      TokenRangeSupplier tokenRangeSupplier,
                                      CdcSidecarInstancesProvider sidecarInstancesProvider,
                                      SecretsProvider secretsProvider,
                                      SidecarCdcClient.ClientConfig clientConfig,
                                      CdcConfig conf,
                                      ICdcStats cdcStats,
                                      TaskExecutorPool taskExecutorPool) throws IOException
    {
        return buildConsumer(conf.jobId(),
                             instanceId,
                             new SidecarCdcOptions(instanceFetcher),
                             clusterConfigProvider,
                             eventConsumer,
                             schemaSupplier,
                             tokenRangeSupplier,
                             sidecarInstancesProvider,
                             clientConfig,
                             secretsProvider,
                             cdcStats,
                             taskExecutorPool);
    }

    public void startConsumers()
    {
        consumers.forEach(SidecarCdc::initSchema);
        consumers.forEach(SidecarCdc::start);
    }

    public void stopConsumers()
    {
        consumers.forEach(SidecarCdc::stop);
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


    public SidecarCdc buildConsumer(@NotNull String jobId,
                                    int partitionId,
                                    CdcOptions cdcOptions,
                                    ClusterConfigProvider clusterConfigProvider,
                                    EventConsumer eventConsumer,
                                    SchemaSupplier schemaSupplier,
                                    TokenRangeSupplier tokenRangeSupplier,
                                    CdcSidecarInstancesProvider sidecarInstancesProvider,
                                    SidecarCdcClient.ClientConfig clientConfig,
                                    SecretsProvider secretsProvider,
                                    ICdcStats cdcStats,
                                    TaskExecutorPool taskExecutorPool) throws IOException
    {

        AsyncExecutor asyncExecutor = new ExecutorPoolsExecutor(taskExecutorPool);

        final SidecarStatePersister sidecarStatePersister = getSidecarStatePersister(cdcOptions, asyncExecutor);
        return (SidecarCdc) SidecarCdc.builder(jobId,
                                               partitionId,
                                               cdcOptions,
                                               clusterConfigProvider,
                                               eventConsumer,
                                               schemaSupplier,
                                               tokenRangeSupplier,
                                               sidecarInstancesProvider,
                                               clientConfig,
                                               secretsProvider,
                                               cdcStats).withExecutor(asyncExecutor).withStatePersister(sidecarStatePersister).build();
    }

    private @NotNull SidecarStatePersister getSidecarStatePersister(CdcOptions cdcOptions, AsyncExecutor asyncExecutor)
    {
        SidecarStatePersister sidecarStatePersister = new SidecarStatePersister(org.apache.cassandra.cdc.sidecar.SidecarCdcOptions.DEFAULT,
                                                                                cdcOptions,
                                                                                SidecarCdcStats.STUB,
                                                                                new StateSidecarCdcCassandraClient(cdcDatabaseAccessor),
                                                                                asyncExecutor);
        sidecarStatePersister.start();
        return sidecarStatePersister;
    }
}
