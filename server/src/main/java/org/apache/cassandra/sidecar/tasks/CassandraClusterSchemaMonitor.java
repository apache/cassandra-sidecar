/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.tasks;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.DriverUnsupportedSchemaCache;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/**
 * Central schema management component for Cassandra cluster schema monitoring and CDC table tracking.
 * This class provides comprehensive schema management functionality for Cassandra Sidecar, specifically
 * focused on CDC (Change Data Capture) operations. It maintains real-time awareness of schema changes
 * in the Cassandra cluster and manages CDC-enabled table metadata.
 */
@Singleton
public class CassandraClusterSchemaMonitor implements PeriodicTask
{
    // 49sec least-common multiple with 60sec is 49min so offers best monitor frequency without clashing with 60sec
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterSchemaMonitor.class);

    private final AtomicReference<String> currSchemaText = new AtomicReference<>("");
    private final AtomicReference<Set<CqlTable>> cdcTables = new AtomicReference<>(Collections.emptySet());
    private final ConcurrentHashMap<TableIdentifier, UUID> tableIdCache = new ConcurrentHashMap<>();
    private final CdcDatabaseAccessor databaseAccessor;
    private final DriverUnsupportedSchemaCache driverUnsupportedSchemaCache;
    private final CopyOnWriteArrayList<Runnable> schemaChangeListeners = new CopyOnWriteArrayList<>();
    private final SidecarConfiguration sidecarConfiguration;
    private final InstanceMetadataFetcher instanceFetcher;
    private final CassandraBridgeFactory cassandraBridgeFactory;

    public CassandraClusterSchemaMonitor(InstanceMetadataFetcher instanceFetcher,
                                         CdcDatabaseAccessor databaseAccessor,
                                         DriverUnsupportedSchemaCache driverUnsupportedSchemaCache,
                                         SidecarConfiguration sidecarConfiguration,
                                         CassandraBridgeFactory cassandraBridgeFactory)
    {

        this.instanceFetcher = instanceFetcher;
        this.databaseAccessor = databaseAccessor;
        this.driverUnsupportedSchemaCache = driverUnsupportedSchemaCache;
        this.sidecarConfiguration = sidecarConfiguration;
        this.cassandraBridgeFactory = cassandraBridgeFactory;
    }

    public void addSchemaChangeListener(Runnable listener)
    {
        schemaChangeListeners.add(listener);
    }

    public void refresh()
    {
        NodeSettings nodeSettings = instanceFetcher.callOnFirstAvailableInstance(instance -> instance.delegate().nodeSettings());
        CassandraBridge cassandraBridge = cassandraBridgeFactory.get(nodeSettings.releaseVersion());
        CdcBridge cdcBridge = CdcBridgeFactory.getCdcBridge(cassandraBridge);

        try
        {
            LOGGER.debug("Checking for schema changes...");
            String fullSchemaText = DriverUnsupportedSchemaCache.concatSchemas(databaseAccessor.fullSchema(),
                                                                               driverUnsupportedSchemaCache.getFullSchema());
            if (!fullSchemaText.equals(currSchemaText.get()))
            {
                LOGGER.info("Schema change detected, refreshing CDC tables");
                currSchemaText.set(fullSchemaText);
                Set<CqlTable> updatedCdcTables = buildCdcTables(fullSchemaText, databaseAccessor, tableIdCache, cassandraBridge);
                LOGGER.info("Cdc enabled tables tables='{}'", 
                            updatedCdcTables.stream()
                                            .map(m -> String.format("%s.%s", m.keyspace(), m.table()))
                                            .collect(Collectors.joining(",")));
                cdcTables.set(updatedCdcTables);

                cdcBridge.updateCdcSchema(updatedCdcTables, getPartitioner(nodeSettings),
                                          ((keyspace, table) -> tableIdCache.get(TableIdentifier.of(keyspace, table))));
                schemaChangeListeners.forEach(Runnable::run);
            }
        }
        catch (IllegalStateException exception)
        {
            LOGGER.warn("There was a problem refreshing the schema. Database Accessor may not be ready", exception);
            throw exception;
        }
        catch (Throwable t)
        {
            LOGGER.error("Unexpected error while refreshing the schema", t);
            throw t;
        }
    }

    public Set<CqlTable> getCdcTables()
    {
        return cdcTables.get();
    }

    private Partitioner getPartitioner(NodeSettings nodeSettings)
    {
        if (nodeSettings.partitioner().contains("."))
        {
            String[] splitPartitionerName = nodeSettings.partitioner().split("\\.");
            return Partitioner.valueOf(splitPartitionerName[splitPartitionerName.length - 1]);
        }
        return Partitioner.valueOf(nodeSettings.partitioner());
    }

    @Override
    public DurationSpec delay()
    {
        return sidecarConfiguration.serviceConfiguration().cdcConfiguration().tableSchemaRefreshTime();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            refresh();
            promise.tryComplete();
        }
        catch (Throwable t)
        {
            promise.fail(t);
        }
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration().isEnabled() &&
            sidecarConfiguration.serviceConfiguration().cdcConfiguration().isEnabled())
        {
            return ScheduleDecision.EXECUTE;
        }
        return ScheduleDecision.SKIP;
    }

    @VisibleForTesting
    static Set<CqlTable> buildCdcTables(CdcDatabaseAccessor cdcDatabaseAccessor,
                                        DriverUnsupportedSchemaCache driverUnsupportedSchemaCache,
                                        ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                        @NotNull final CassandraBridge cassandraBridge)
    {
        String fullSchema = DriverUnsupportedSchemaCache.concatSchemas(cdcDatabaseAccessor.fullSchema(),
                                                                       driverUnsupportedSchemaCache.getFullSchema());
        return buildCdcTables(fullSchema,
                              cdcDatabaseAccessor.partitioner(),
                              tableIdCache,
                              cdcDatabaseAccessor::getTableId,
                              cassandraBridge);
    }

    private static Set<CqlTable> buildCdcTables(@NotNull String fullSchema,
                                                @NotNull CdcDatabaseAccessor cdcDatabaseAccessor,
                                                @NotNull ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                                @NotNull final CassandraBridge cassandraBridge)
    {
        return buildCdcTables(fullSchema,
                              cdcDatabaseAccessor.partitioner(),
                              tableIdCache,
                              cdcDatabaseAccessor::getTableId,
                              cassandraBridge);
    }

    private static Set<CqlTable> buildCdcTables(@NotNull String fullSchema,
                                                @NotNull Partitioner partitioner,
                                                @NotNull ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                                @NotNull Function<TableIdentifier, UUID> tableIdLoaderFunction,
                                                @NotNull CassandraBridge cassandraBridge)
    {
        Map<TableIdentifier, String> createStmts = CdcUtil.extractCdcTables(fullSchema);
        Map<String, Set<String>> udtsPerKeyspace = createStmts.keySet()
                                                              .stream()
                                                              .map(TableIdentifier::keyspace)
                                                              .distinct() // remove duplicated keyspace strings
                                                              .collect(Collectors.toMap(Function.identity(),
                                                                                        keyspace -> CqlUtils.extractUdts(fullSchema, keyspace)));

        Map<TableIdentifier, UUID> tableIds = createStmts.keySet()
                                                         .stream()
                                                         .collect(Collectors.toMap(Function.identity(),
                                                                                   id -> tableIdCache.computeIfAbsent(id, tableIdLoaderFunction)));

        return createStmts.entrySet().stream()
                          .map(e ->
                               {
                                   TableIdentifier id = e.getKey();
                                   ReplicationFactor rf = CqlUtils.extractReplicationFactor(fullSchema, id.keyspace());

                                   return cassandraBridge.buildSchema(e.getValue(), id.keyspace(), rf,
                                                                      partitioner, udtsPerKeyspace.get(id.keyspace()),
                                                                      tableIds.get(id), 0, true);
                               })
                          .collect(Collectors.toSet());
    }
}
