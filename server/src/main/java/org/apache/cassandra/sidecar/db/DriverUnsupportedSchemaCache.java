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

package org.apache.cassandra.sidecar.db;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.utils.StringUtils;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * The {@link DriverUnsupportedSchemaCache} class maintains cache of CQL schema for tables whose definition is not
 * supported by driver natively. Java driver 3.x does not return {@link TableMetadata} for tables which schema
 * could not be parsed. Since it does not currently support vector type, any table containing vector would not
 * be visible. Cache is refreshed for the first time as soon as CQL connection is established or upon first lookup.
 * Later, it is periodically refreshed according to configured schedule.
 * TODO: Remove after upgrade to Java driver 4.x (CASSSIDECAR-421).
 */
@Singleton
public class DriverUnsupportedSchemaCache implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverUnsupportedSchemaCache.class);
    private static final String STATEMENT_DELIMITER = "\n\n";

    private final SidecarConfiguration sidecarConfiguration;
    private final CQLSessionProvider sessionProvider;
    private final CQLSchemaAccessor schemaAccessor;

    // cache contains only schemas unparseable by Java driver
    // during every cache refresh, reference is being reassigned to achieve atomic swap
    private volatile SortedMap<QualifiedTableName, String> schemaCache;
    private volatile boolean initialized; // flag indicating whether cache has been populated at least once

    private PreparedStatement tableListStatement;

    public DriverUnsupportedSchemaCache(SidecarConfiguration sidecarConfiguration,
                                        CQLSessionProvider sessionProvider)
    {
        this.sidecarConfiguration = sidecarConfiguration;
        this.sessionProvider = sessionProvider;
        this.schemaAccessor = new CQLSchemaAccessor(sessionProvider);
        this.schemaCache = createCache();
        this.initialized = false;
    }

    /**
     * @return Schema for all tables across all keyspaces (only those not supported by Java driver).
     */
    @NotNull
    public String getFullSchema()
    {
        // we cannot know whether schema was updated after last
        // periodic refresh, so results may be stale
        refreshIfUninitialized();
        return getUnsupportedSchema(table -> true);
    }

    /**
     * @return Schema for all tables within given keyspaces (only those not supported by Java driver).
     */
    @NotNull
    public String getKeyspaceSchema(@NotNull Name keyspace)
    {
        // we cannot know whether schema was updated after last
        // periodic refresh, so results may be stale
        refreshIfUninitialized();
        return getUnsupportedSchema(table -> keyspace.equals(table.getKeyspace()));
    }

    /**
     * @return Schema for table not supported by Java driver's metadata, {@code null} otherwise.
     */
    @Nullable
    public String getTableSchema(@NotNull Name keyspace, @NotNull Name table)
    {
        return getTableSchema(keyspace, table, true);
    }

    /**
     * @param allowRefresh Flag indicating whether lookup of table schema via CQL query
     *                     is allowed, when data not found in cache.
     * @return Schema for table not supported by Java driver's metadata, {@code null} otherwise.
     */
    @Nullable
    public String getTableSchema(@NotNull Name keyspace, @NotNull Name table, boolean allowRefresh)
    {
        refreshIfUninitialized();
        QualifiedTableName name = new QualifiedTableName(keyspace, table);
        String schema = schemaCache.get(name);
        if (schema == null && allowRefresh)
        {
            // proactively fetch table schema, not to provide
            // false-negative when table is not cached yet
            // attention, method can block
            schema = populateSchemaCache(schemaCache, name);
        }
        return schema;
    }

    @Override
    public DurationSpec delay()
    {
        return sidecarConfiguration.driverConfiguration().unsupportedTableSchemaRefreshTime();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            refresh(false);
            promise.tryComplete();
        }
        catch (Throwable t)
        {
            promise.fail(t);
        }
    }

    private String getUnsupportedSchema(Predicate<QualifiedTableName> condition)
    {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<QualifiedTableName, String> entry : schemaCache.entrySet())
        {
            if (condition.test(entry.getKey()))
            {
                result.append(entry.getValue()).append(STATEMENT_DELIMITER);
            }
        }
        return result.toString().trim();
    }

    private void refreshIfUninitialized()
    {
        if (!initialized)
        {
            refresh(true);
        }
    }

    public synchronized void refresh(boolean initializeOnly)
    {
        if (initialized && initializeOnly)
        {
            // cache has been already initialized, early exit
            return;
        }
        try
        {
            Session session = sessionProvider.get();
            prepareStatements(session);

            Set<QualifiedTableName> tables = queryAllTables(session);
            Set<QualifiedTableName> driverKnownTables = driverKnownTables(session);

            tables.removeAll(driverKnownTables);

            SortedMap<QualifiedTableName, String> newCache = createCache();
            if (!tables.isEmpty())
            {
                LOGGER.debug("Tables not supported by Java driver metadata: {}", tables);
                tables.forEach(table -> populateSchemaCache(newCache, table));
            }
            // replacing cache, because some tables might have been removed in the meanwhile
            schemaCache = newCache;

            initialized = true;
        }
        catch (CassandraUnavailableException ignored)
        {
            LOGGER.debug("Not yet connected to Cassandra cluster");
        }
    }

    private String populateSchemaCache(Map<QualifiedTableName, String> cache, QualifiedTableName table)
    {
        Name keyspaceName = table.getKeyspace();
        Name tableName = table.table();
        if (keyspaceName == null || tableName == null)
        {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
        List<String> cqlSchema = schemaAccessor.getTableSchema(keyspaceName, tableName);
        if (cqlSchema != null)
        {
            String schema = String.join(STATEMENT_DELIMITER, cqlSchema);
            cache.put(table, schema);
            return schema;
        }
        return null;
    }

    private Set<QualifiedTableName> queryAllTables(Session session)
    {
        List<Row> rows = session.execute(tableListStatement.bind()).all();
        return rows.stream()
                   .map(r -> new QualifiedTableName(r.getString("keyspace_name"),
                                                    r.getString("table_name")))
                   .collect(Collectors.toSet());
    }

    private Set<QualifiedTableName> driverKnownTables(Session session)
    {
        Set<QualifiedTableName> result = new HashSet<>();
        Cluster cluster = session.getCluster();
        for (KeyspaceMetadata keyspace : cluster.getMetadata().getKeyspaces())
        {
            for (TableMetadata table : keyspace.getTables())
            {
                result.add(new QualifiedTableName(keyspace.getName(), table.getName()));
            }
        }
        return result;
    }

    private void prepareStatements(Session session)
    {
        if (tableListStatement == null)
        {
            tableListStatement = session.prepare("SELECT keyspace_name, table_name FROM system_schema.tables");
        }
    }

    private SortedMap<QualifiedTableName, String> createCache()
    {
        // use sorted map for repeatable results when retrieving full schema
        // synchronized, because different threads may indirectly add items to the map using getTableSchema() method
        return Collections.synchronizedSortedMap(new TreeMap<>(Comparator.comparing(QualifiedTableName::toString)));
    }

    @VisibleForTesting
    void setInitialized(boolean initialized)
    {
        this.initialized = initialized;
    }

    public static String concatSchemas(String ... schemas)
    {
        StringBuilder result = new StringBuilder();
        for (String schema : schemas)
        {
            if (StringUtils.isNotEmpty(schema))
            {
                result.append(schema.trim()).append(STATEMENT_DELIMITER);
            }
        }
        return result.toString().trim();
    }
}
