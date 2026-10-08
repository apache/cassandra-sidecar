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

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
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
 * The {@link SchemaCache} class maintains cache of keyspace, table schemas read through {@code DESCRIBE} call. Schema
 * provided by Java driver could be partial as it drops objects it cannot parse. It also stores driver unparseable
 * table level schemas, used for table existence checks.
 * <p>
 * Cache is regularly refreshed, but schema changes applied between refreshes are may not be immediately visible
 */
@Singleton
public class SchemaCache implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCache.class);
    private static final String STATEMENT_DELIMITER = "\n\n";

    private final SidecarConfiguration sidecarConfiguration;
    private final CQLSessionProvider sessionProvider;
    private final CQLSchemaAccessor schemaAccessor;

    // Caches table schemas those that could not be parsed by Java driver
    private volatile ConcurrentNavigableMap<QualifiedTableName, String> unsupportedTableSchemaCache;
    private volatile ConcurrentNavigableMap<String, String> keyspaceSchemaCache;
    // CQL schema of all keyspaces. It is invalidated whenever the schema of a keyspace is added to keyspaceSchemaCache
    private final AtomicReference<String> fullSchemaCache = new AtomicReference<>();
    private volatile boolean initialized;

    private PreparedStatement tableListStatement;

    public SchemaCache(SidecarConfiguration sidecarConfiguration, CQLSessionProvider sessionProvider)
    {
        this.sidecarConfiguration = sidecarConfiguration;
        this.sessionProvider = sessionProvider;
        this.schemaAccessor = new CQLSchemaAccessor(sessionProvider);
        this.unsupportedTableSchemaCache = createTableSchemaCache();
        this.keyspaceSchemaCache = createKeyspaceSchemaCache();
        this.initialized = false;
    }

    /**
     * @return CQL schema of all keyspaces. Keyspaces created after the last cache refresh are not included, unless
     * their schema has been looked up individually in the meanwhile, so results may be stale.
     */
    @NotNull
    public String getFullSchema()
    {
        refreshIfUninitialized();
        String fullSchema = fullSchemaCache.get();
        if (fullSchema == null)
        {
            fullSchema = assembleFullSchema(keyspaceSchemaCache);
            fullSchemaCache.compareAndSet(null, fullSchema);
        }
        // The locally assembled schema is still returned, because the cache may have been invalidated again
        // by the time the set fails
        return fullSchema;
    }

    /**
     * @return CQL schema of the keyspace, {@code null} when the keyspace does not exist. If the keyspace was updated
     * after last refresh, results maybe stale until next refresh.
     */
    @Nullable
    public String getKeyspaceSchema(@NotNull String keyspace)
    {
        refreshIfUninitialized();
        ConcurrentNavigableMap<String, String> cache = keyspaceSchemaCache;
        String schema = cache.get(keyspace);
        if (schema == null)
        {
            schema = populateKeyspaceSchemaCache(cache, keyspace);
        }
        return schema;
    }

    /**
     * @return Schema for all tables across all keyspaces (only those not supported by Java driver). we cannot know
     * whether schema was updated after last periodic refresh, so results may be stale.
     */
    @NotNull
    public String getUnsupportedTableSchema()
    {
        refreshIfUninitialized();
        StringBuilder result = new StringBuilder();
        for (Map.Entry<QualifiedTableName, String> entry : unsupportedTableSchemaCache.entrySet())
        {
            result.append(entry.getValue()).append(STATEMENT_DELIMITER);
        }
        return result.toString().trim();
    }

    /**
     * @return Schema for table not supported by Java driver's metadata, {@code null} otherwise.
     */
    @Nullable
    public String getUnsupportedTableSchema(@NotNull Name keyspace, @NotNull Name table)
    {
        return getUnsupportedTableSchema(keyspace, table, true);
    }

    /**
     * @param allowRefresh Flag indicating whether lookup of table schema via CQL query
     *                     is allowed, when data not found in cache.
     * @return Schema for table not supported by Java driver's metadata, {@code null} otherwise.
     */
    @Nullable
    public String getUnsupportedTableSchema(@NotNull Name keyspace, @NotNull Name table, boolean allowRefresh)
    {
        refreshIfUninitialized();
        // the cache is keyed by the names as stored by Cassandra
        QualifiedTableName name = new QualifiedTableName(foldIfUnquoted(keyspace), foldIfUnquoted(table));
        String schema = unsupportedTableSchemaCache.get(name);
        if (schema == null && allowRefresh)
        {
            // proactively fetch table schema, not to provide
            // false-negative when table is not cached yet
            // attention, method can block
            schema = populateTableSchemaCache(unsupportedTableSchemaCache, name);
        }
        return schema;
    }

    @Override
    public DurationSpec delay()
    {
        return sidecarConfiguration.driverConfiguration().schemaRefreshTime();
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
            return;
        }
        try
        {
            Session session = sessionProvider.get();
            prepareStatements(session);

            Set<QualifiedTableName> tables = queryAllTables(session);
            Set<QualifiedTableName> driverKnownTables = driverKnownTables(session);
            tables.removeAll(driverKnownTables);

            ConcurrentNavigableMap<QualifiedTableName, String> newTableSchemaCache = createTableSchemaCache();
            if (!tables.isEmpty())
            {
                LOGGER.debug("Tables not supported by Java driver metadata: {}", tables);
                tables.forEach(table -> populateTableSchemaCache(newTableSchemaCache, table));
            }
            unsupportedTableSchemaCache = newTableSchemaCache;

            ConcurrentNavigableMap<String, String> newKeyspaceSchemaCache = createKeyspaceSchemaCache();
            for (Name keyspace : schemaAccessor.getKeyspaces())
            {
                populateKeyspaceSchemaCache(newKeyspaceSchemaCache, keyspace.name());
            }
            keyspaceSchemaCache = newKeyspaceSchemaCache;
            fullSchemaCache.set(assembleFullSchema(newKeyspaceSchemaCache));
            initialized = true;
        }
        catch (CassandraUnavailableException ignored)
        {
            LOGGER.debug("Error refreshing SchemaCache, not yet connected to Cassandra");
        }
    }

    @Nullable
    private String populateKeyspaceSchemaCache(Map<String, String> cache, String keyspace)
    {
        List<String> cqlSchema = schemaAccessor.getKeyspaceSchema(new Name(Metadata.quoteIfNecessary(keyspace)));
        if (cqlSchema == null || cqlSchema.isEmpty())
        {
            return null;
        }
        String schema = String.join(STATEMENT_DELIMITER, cqlSchema);
        cache.put(keyspace, schema);
        fullSchemaCache.set(null);
        return schema;
    }

    private static String assembleFullSchema(Map<String, String> keyspaceSchemas)
    {
        StringBuilder result = new StringBuilder();
        for (String keyspaceSchema : keyspaceSchemas.values())
        {
            result.append(keyspaceSchema).append(STATEMENT_DELIMITER);
        }
        return result.toString().trim();
    }

    private String populateTableSchemaCache(Map<QualifiedTableName, String> cache, QualifiedTableName table)
    {
        Name keyspaceName = table.getKeyspace();
        Name tableName = table.table();
        if (keyspaceName == null || tableName == null)
        {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
        // Cassandra stores the names unquoted, while DESCRIBE requires them to be quoted when the name is case
        // sensitive or a reserved keyword
        List<String> cqlSchema = schemaAccessor.getTableSchema(new Name(Metadata.quoteIfNecessary(keyspaceName.name())),
                                                               new Name(Metadata.quoteIfNecessary(tableName.name())));
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

    /**
     * Normalizes a name obtained from a request into the form Cassandra stores it in: CQL folds unquoted
     * names to lowercase, while quoted names keep their case.
     */
    private static String foldIfUnquoted(Name name)
    {
        return name.isSourceQuoted() ? name.name() : name.name().toLowerCase(Locale.ROOT);
    }

    private ConcurrentNavigableMap<QualifiedTableName, String> createTableSchemaCache()
    {
        // uses ConcurrentSkipListMap to have assembled full schema or table schemas consistent across calls
        return new ConcurrentSkipListMap<>(Comparator.comparing(QualifiedTableName::toString));
    }

    private ConcurrentNavigableMap<String, String> createKeyspaceSchemaCache()
    {
        // uses ConcurrentSkipListMap to have assembled full schema or table schemas consistent across calls
        return new ConcurrentSkipListMap<>();
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
