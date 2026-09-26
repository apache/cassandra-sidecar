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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Promise;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.cdc.CdcSchemaUtils;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.DriverUnsupportedSchemaCache;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TableIdentifier;

/**
 * Central schema management component for Cassandra cluster schema monitoring and CDC table tracking.
 * This class provides comprehensive schema management functionality for Cassandra Sidecar, specifically
 * focused on CDC (Change Data Capture) operations. It maintains real-time awareness of schema changes
 * in the Cassandra cluster and manages CDC-enabled table metadata.
 *
 * <p><b>Sole owner of live table un-registration:</b> a separate, independent schema-refresh
 * loop also exists inside every {@code SidecarCdc} consumer instance (started by
 * {@code CdcManager}/{@code CdcPublisher}; see {@code org.apache.cassandra.cdc.Cdc#refreshSchema}
 * in cassandra-analytics), on its own cadence ({@code CdcOptions#schemaRefreshDelay()}, not tied
 * to {@link #delay()} here). Both loops source their table set from the same
 * {@code CdcSchemaSupplier} / {@link org.apache.cassandra.sidecar.cdc.CdcBatchRiskAnalyzer}
 * computation and call {@code CdcBridge#updateCdcSchema} — but only <em>this</em> class calls
 * {@code CdcBridge#unregisterNonCdcTables} to remove a table once it's no longer at risk of a CDC
 * batch. This is intentional, not an oversight: {@code Cdc.refreshSchema()} is shared code used
 * by non-sidecar CDC consumers that have no equivalent monitor, so it was left register-only.
 * In a normal sidecar deployment both loops run, and re-registration by the other loop never
 * produces an incorrect result (a CDC-enabled table is never touched by unregistration) — but it
 * does mean the deserialization-cost savings from unregistering a now-safe table only fully
 * materialize once this class's loop is the one steering steady-state registration.
 */
public class CassandraClusterSchemaMonitor implements PeriodicTask
{
    // 49sec least-common multiple with 60sec is 49min so offers best monitor frequency without clashing with 60sec
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterSchemaMonitor.class);

    private final AtomicReference<String> currSchemaText = new AtomicReference<>("");
    private final AtomicReference<Set<CqlTable>> cdcTables = new AtomicReference<>(Collections.emptySet());
    private final AtomicReference<Set<TableIdentifier>> lastRegisteredTables = new AtomicReference<>(Collections.emptySet());
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

                // Build the set of user tables that need to be registered (CDC-enabled tables
                // plus any non-CDC table that shares partition-key structure with one, when
                // batch statements are possible — see CdcBatchRiskAnalyzer). CDC-only tables
                // are derived by filtering on CqlTable.cdc() — avoids a separate schema parse
                // pass.
                boolean batchStatementsEnabled = sidecarConfiguration.serviceConfiguration().cdcConfiguration().batchStatementsEnabled();
                Set<CqlTable> tablesToRegisterForCDC = CdcSchemaUtils.buildTablesToRegisterForCDC(fullSchemaText,
                                                                                getPartitioner(nodeSettings),
                                                                                tableIdCache,
                                                                                databaseAccessor::getTableId,
                                                                                cassandraBridge,
                                                                                batchStatementsEnabled);
                Set<CqlTable> updatedCdcTables = tablesToRegisterForCDC.stream()
                                                                       .filter(CqlTable::cdc)
                                                                       .collect(Collectors.toSet());
                LOGGER.info("Cdc enabled tables tables='{}'",
                            updatedCdcTables.stream()
                                            .map(m -> String.format("%s.%s", m.keyspace(), m.table()))
                                            .collect(Collectors.joining(",")));
                cdcTables.set(updatedCdcTables);

                // Pass the computed table set to updateCdcSchema so Schema.instance has enough
                // tables to deserialize commit log mutations without throwing
                // UnknownTableException — how much of the schema that covers depends on
                // batchStatementsEnabled (see above).
                cdcBridge.updateCdcSchema(tablesToRegisterForCDC, getPartitioner(nodeSettings),
                                          ((keyspace, table) -> tableIdCache.get(TableIdentifier.of(keyspace, table))));

                // Unregister any table that was registered by a previous refresh but is no
                // longer needed (e.g. the CDC-enabled table it was at risk of batching with was
                // dropped, or either table's partition key was altered so they no longer share
                // structure). Always register/update BEFORE unregistering, so there is never a
                // window where a still-needed table is missing from Schema.instance.
                Set<TableIdentifier> newlyRegisteredIds = tablesToRegisterForCDC.stream()
                                                                                .map(t -> TableIdentifier.of(t.keyspace(), t.table()))
                                                                                .collect(Collectors.toSet());
                Set<TableIdentifier> staleIds = new HashSet<>(lastRegisteredTables.get());
                staleIds.removeAll(newlyRegisteredIds);
                if (!staleIds.isEmpty())
                {
                    LOGGER.info("Unregistering tables no longer at risk of a CDC batch tables='{}'",
                                staleIds.stream()
                                        .map(id -> String.format("%s.%s", id.keyspace(), id.table()))
                                        .collect(Collectors.joining(",")));
                    try
                    {
                        cdcBridge.unregisterNonCdcTables(staleIds);
                    }
                    catch (Throwable t)
                    {
                        // Don't lose track of tables we failed to unregister — if we recorded
                        // lastRegisteredTables as newlyRegisteredIds regardless, a permanently
                        // stale table would never be retried on a future refresh (staleIds is
                        // computed as a diff against lastRegisteredTables). Keep staleIds in the
                        // tracked set instead, so the next refresh attempts them again.
                        LOGGER.warn("Failed to unregister stale CDC batch tables, will retry on next refresh tables='{}'",
                                    staleIds.stream()
                                            .map(id -> String.format("%s.%s", id.keyspace(), id.table()))
                                            .collect(Collectors.joining(",")), t);
                        newlyRegisteredIds = new HashSet<>(newlyRegisteredIds);
                        newlyRegisteredIds.addAll(staleIds);
                    }
                }
                lastRegisteredTables.set(newlyRegisteredIds);

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

    /**
     * @return the full set of tables currently registered in the CDC bridge's schema as of the
     * last refresh — CDC-enabled tables plus any non-CDC table at risk of a batch with one (see
     * {@link org.apache.cassandra.sidecar.cdc.CdcBatchRiskAnalyzer}). Exposed primarily for
     * tests to assert on what is/isn't registered without a CDC bridge reference of their own.
     */
    @VisibleForTesting
    public Set<TableIdentifier> getRegisteredTables()
    {
        return lastRegisteredTables.get();
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

}
