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
 * The {@link SchemaCache} class maintains cache of CQL schema read from Cassandra with server side
 * {@code DESCRIBE} statements. It holds three kinds of schema:
 * <ul>
 *     <li>The complete schema of every keyspace, as rendered by Cassandra itself, and the schema of all keyspaces
 *     assembled from it. It is the schema served by the schema routes, and it is preferred over the schema rendered
 *     by Java driver, which drops the objects it cannot parse and renders only a fixed subset of the keyspace and
 *     table options. It also stores driver unparseable table level schemas, it is used for table existence checks.</li>
 * </ul>
 * Cache is refreshed for the first time as soon as CQL connection is established or upon first lookup.
 * Later, it is periodically refreshed according to configured schedule, therefore schema changes applied after the
 * last refresh are not visible, except for keyspaces which are looked up for the first time.
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
    // CQL schema of all keyspaces, assembled from keyspaceSchemaCache once and not on every request
    // it is invalidated whenever the schema of a keyspace is added to keyspaceSchemaCache
    private final AtomicReference<String> fullSchemaCache = new AtomicReference<>();
    // flag indicating whether cache has been populated at least once
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
     * @return CQL schema of all keyspaces, as rendered by Cassandra. Keyspaces created after the last cache refresh
     * are not included, unless their schema has been looked up individually in the meanwhile, so results may be stale.
     */
    @NotNull
    public String getSchema()
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
     * @param keyspace the unquoted keyspace name, as stored by Cassandra
     * @return CQL schema of the keyspace, as rendered by Cassandra, {@code null} when the keyspace does not exist or
     * its schema could not be read. If the keyspace was updated after last refresh, results maybe stale until next
     * refresh.
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
        QualifiedTableName name = new QualifiedTableName(keyspace, table);
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

            ConcurrentNavigableMap<QualifiedTableName, String> newTableSchemaCache = createTableSchemaCache();
            if (!tables.isEmpty())
            {
                LOGGER.debug("Tables not supported by Java driver metadata: {}", tables);
                tables.forEach(table -> populateTableSchemaCache(newTableSchemaCache, table));
            }
            // replacing cache, because some tables might have been removed in the meanwhile
            unsupportedTableSchemaCache = newTableSchemaCache;

            ConcurrentNavigableMap<String, String> newKeyspaceSchemaCache = createKeyspaceSchemaCache();
            for (Name keyspace : schemaAccessor.getKeyspaces())
            {
                populateKeyspaceSchemaCache(newKeyspaceSchemaCache, keyspace.name());
            }
            // replacing cache, because some keyspaces might have been dropped in the meanwhile
            keyspaceSchemaCache = newKeyspaceSchemaCache;
            fullSchemaCache.set(assembleFullSchema(newKeyspaceSchemaCache));

            initialized = true;
        }
        catch (CassandraUnavailableException ignored)
        {
            LOGGER.debug("Not yet connected to Cassandra cluster");
        }
    }

    @Nullable
    private String populateKeyspaceSchemaCache(Map<String, String> cache, String keyspace)
    {
        // Cassandra stores the keyspace name unquoted, while DESCRIBE requires it to be quoted
        // when the name is case sensitive or a reserved keyword
        List<String> cqlSchema = schemaAccessor.getKeyspaceSchema(new Name(Metadata.quoteIfNecessary(keyspace)));
        if (cqlSchema == null || cqlSchema.isEmpty())
        {
            return null;
        }
        String schema = String.join(STATEMENT_DELIMITER, cqlSchema);
        cache.put(keyspace, schema);
        // the schema of all keyspaces has to be assembled again, note that a cache refresh assembles it
        // eagerly, right after it has populated the schema of every keyspace
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

    private ConcurrentNavigableMap<QualifiedTableName, String> createTableSchemaCache()
    {
        // sorted for repeatable results when the schema of the unsupported tables is assembled, and concurrent,
        // because different threads may add items to the map using getTableSchema() method while it is iterated
        return new ConcurrentSkipListMap<>(Comparator.comparing(QualifiedTableName::toString));
    }

    private ConcurrentNavigableMap<String, String> createKeyspaceSchemaCache()
    {
        // sorted for repeatable results when the schema of all keyspaces is assembled, and concurrent, because
        // different threads may add items to the map using getKeyspaceSchema() method while it is iterated
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
