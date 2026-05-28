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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.cassandra.cdc.avro.AvroSchemaUtils;
import org.apache.cassandra.cdc.avro.AvroSchemas;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.cdc.kafka.KafkaOptions;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.cdc.schemastore.SchemaStorePublisherFactory;
import org.apache.cassandra.cdc.schemastore.TableSchemaPublisher;
import org.apache.cassandra.sidecar.db.TableHistoryDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.tasks.CassandraClusterSchemaMonitor;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.utils.TableIdentifier;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CACHE_WARMED_UP;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_START;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;

/**
 * Schemas cache for CDC event serialization, keyed by TableIdentifier.
 *
 * <p>{@link #getSchema}, {@link #getWriter}, and {@link #getReader} all return the
 * <em>payload</em> (table-column) Avro schema — not the CDC envelope. Every caller in the
 * analytics codec uses these to build and encode per-row payload records (column field lookup,
 * range predicate encoding, byte-record encoding). Returning the merged envelope schema here
 * would break those callers because table columns are not top-level fields of the envelope.
 *
 * <p>The merged CDC envelope schema is constructed on-the-fly in {@link #publishSchemas} and
 * is never stored in the cache. The sidecar remains the authoritative source for building the
 * merged schema (via {@link #buildMergedSchema}); it is just not exposed through
 * {@link SchemaStore#getSchema}.
 *
 * <p>Schema version UUIDs ({@link #getVersion}) are derived from the CQL {@code CREATE TABLE}
 * statement, consistent with the analytics {@code CachingSchemaStore} implementation.
 */
public class CachingSchemaStore implements SchemaStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CachingSchemaStore.class);
    private final Map<TableIdentifier, SchemaCacheEntry> avroSchemasCache = new ConcurrentHashMap<>();
    private final CassandraClusterSchemaMonitor cassandraClusterSchemaMonitor;
    private final SidecarSchema sidecarSchema;
    private final TableHistoryDatabaseAccessor tableHistoryDatabaseAccessor;
    private final Vertx vertx;
    private final CdcConfigImpl cdcConfig;
    @Nullable private volatile TableSchemaPublisher publisher;
    private final CqlToAvroSchemaConverter cqlToAvroSchemaConverter;
    private final SidecarCdcStats sidecarCdcStats;
    private final SchemaStorePublisherFactory schemaStorePublisherFactory;

    private static final String METADATA_NAME_KEY = "name";
    private static final String METADATA_NAMESPACE_KEY = "namespace";

    @Inject
    CachingSchemaStore(Vertx vertx,
                       CassandraClusterSchemaMonitor cassandraClusterSchemaMonitor,
                       TableHistoryDatabaseAccessor tableHistoryDatabaseAccessor,
                       CdcConfigImpl cdcConfig,
                       SidecarCdcStats sidecarCdcStats,
                       SidecarSchema sidecarSchema,
                       CqlToAvroSchemaConverter cqlToAvroSchemaConverter,
                       SchemaStorePublisherFactory schemaStorePublisherFactory)
    {
        this.cassandraClusterSchemaMonitor = cassandraClusterSchemaMonitor;
        this.tableHistoryDatabaseAccessor = tableHistoryDatabaseAccessor;
        this.sidecarSchema = sidecarSchema;
        this.cqlToAvroSchemaConverter = cqlToAvroSchemaConverter;
        this.vertx = vertx;
        this.cdcConfig = cdcConfig;
        this.sidecarCdcStats = sidecarCdcStats;
        this.schemaStorePublisherFactory = schemaStorePublisherFactory;
        // cdcConfig and schemaStorePublisherFactory must be assigned before the calls below.
        // createSchemaCache and addSchemaChangeListener can fire onSchemaChanged synchronously,
        // which calls loadPublisher() — accessing both fields — and would throw NPE if they
        // were still null at that point.
        this.avroSchemasCache.putAll(createSchemaCache(cassandraClusterSchemaMonitor.getCdcTables()));
        AvroSchemas.registerLogicalTypes();
        cassandraClusterSchemaMonitor.addSchemaChangeListener(this::onSchemaChanged);

        if (cdcConfig.cdcEnabled())
        {
            configureSidecarServerEventListeners();
        }
    }

    private void loadPublisher()
    {
        KafkaOptions kafkaOptions = cdcConfig::kafkaConfigs;
        if (this.publisher != null)
        {
            this.publisher.close();
        }
        this.publisher = schemaStorePublisherFactory.buildPublisher(kafkaOptions);
    }

    private void configureSidecarServerEventListeners()
    {
        EventBus eventBus = vertx.eventBus();

        eventBus.localConsumer(ON_SERVER_START.address(), startMessage -> {
            eventBus.localConsumer(ON_SIDECAR_SCHEMA_INITIALIZED.address(), message -> {
                LOGGER.debug("Sidecar Schema initialized message={}", message);
                Set<CqlTable> refreshedCdcTables = cassandraClusterSchemaMonitor.getCdcTables();
                for (CqlTable cqlTable : refreshedCdcTables)
                {
                    TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
                    avroSchemasCache.compute(tableIdentifier, (k, v) ->
                    {
                        tableHistoryDatabaseAccessor.insertTableSchemaHistory(cqlTable.keyspace(), cqlTable.table(), cqlTable.createStatement());
                        return v;
                    });
                }
                try
                {
                    loadPublisher();
                    publishSchemas();
                }
                catch (Exception e)
                {
                    LOGGER.error("Failed to publish schemas to Kafka during initialization, CDC will still start", e);
                }
            });
        });
    }

    private void publishSchemas()
    {
        Set<CqlTable> refreshedCdcTables = cassandraClusterSchemaMonitor.getCdcTables();
        for (CqlTable cqlTable : refreshedCdcTables)
        {
            TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
            avroSchemasCache.compute(tableIdentifier, (k, v) ->
            {
                Schema payloadSchema = cqlToAvroSchemaConverter.convert(cqlTable);
                if (null != publisher)
                {
                    // Merged schema is only needed for Kafka publishing — build on-the-fly,
                    // not stored in the cache entry (getSchema/getWriter/getReader use payload).
                    Schema mergedSchema = buildMergedSchema(payloadSchema, cqlTable);
                    TableSchemaPublisher.SchemaPublishMetadata metadata = new TableSchemaPublisher.SchemaPublishMetadata();
                    metadata.put(METADATA_NAME_KEY, cqlTable.table());
                    metadata.put(METADATA_NAMESPACE_KEY, cqlTable.keyspace());
                    publisher.publishSchema(mergedSchema.toString(false), metadata);
                    sidecarCdcStats.capturePublishedSchema();
                }
                return new SchemaCacheEntry(cqlTable, payloadSchema);
            });
        }
    }

    private Schema buildMergedSchema(Schema payloadSchema, CqlTable cqlTable)
    {
        String prefix = cdcConfig.schemaNamespacePrefix();
        String namespace = prefix.isEmpty()
                           ? cqlTable.keyspace()
                           : prefix + '.' + cqlTable.keyspace();
        return AvroSchemaUtils.buildMergedSchema(payloadSchema, cqlTable.table(), namespace);
    }

    @VisibleForTesting
    void onSchemaChanged()
    {
        Set<CqlTable> refreshedCdcTables = cassandraClusterSchemaMonitor.getCdcTables();
        for (CqlTable cqlTable : refreshedCdcTables)
        {
            TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
            avroSchemasCache.compute(tableIdentifier, (k, v) ->
            {
                if (v == null || !v.tableSchema().equals(cqlTable.createStatement()))
                {
                    if (sidecarSchema.isInitialized())
                    {
                        tableHistoryDatabaseAccessor.insertTableSchemaHistory(cqlTable.keyspace(), cqlTable.table(), cqlTable.createStatement());
                    }
                    LOGGER.info("Re-generating Avro Schema after schema change keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
                    return new SchemaCacheEntry(cqlTable, cqlToAvroSchemaConverter.convert(cqlTable));
                }
                return v;
            });
        }
        try
        {
            loadPublisher();
            publishSchemas();
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to publish schemas to Kafka, CDC will still start", e);
        }
        // Remove any old schema entries for deleted tables, this operation can be done in the end as this is
        // only for removing stale entries and no one is going to use these entries once the table is removed.
        // This doesn't have to be an atomic operation.
        Set<TableIdentifier> toKeep = refreshedCdcTables.stream()
                                                        .map(cqlTable -> TableIdentifier.of(cqlTable.keyspace(), cqlTable.table()))
                                                        .collect(Collectors.toSet());
        avroSchemasCache.keySet().retainAll(toKeep);
        vertx.eventBus().publish(ON_CDC_CACHE_WARMED_UP.address(), "Cdc cache warmed up");
    }

    @Override
    public Schema getSchema(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k ->
        {
            LOGGER.warn("Unknown table for getting schema keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get schema for unknown table " + tableIdentifier);
        }).schema;
    }

    @Override
    public GenericDatumWriter<GenericRecord> getWriter(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k -> {
            LOGGER.warn("Unknown table for getting writer keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get writer for unknown table " + tableIdentifier);
        }).writer;
    }

    @Override
    public GenericDatumReader<GenericRecord> getReader(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k -> {
            LOGGER.warn("Unknown table for getting reader keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get reader for unknown table " + tableIdentifier);
        }).reader;
    }

    @Override
    public String getVersion(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k -> {
            LOGGER.warn("Unknown table for getting reader keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get reader for unknown table " + tableIdentifier);
        }).schemaUuid;
    }

    private Map<TableIdentifier, SchemaCacheEntry> createSchemaCache(Set<CqlTable> cdcTables)
    {

        return cdcTables.stream()
                        .collect(Collectors.toMap(cqlTable -> TableIdentifier.of(cqlTable.keyspace(), cqlTable.table()),
                                                  cqlTable -> new SchemaCacheEntry(cqlTable, cqlToAvroSchemaConverter.convert(cqlTable)))
                        );
    }

    private TableIdentifier getTableIdentifierFromNamespace(String namespace)
    {
        String[] namespaceParts = namespace.split("\\.");
        return TableIdentifier.of(namespaceParts[0], namespaceParts[1]);
    }

    private static class SchemaCacheEntry
    {
        private final CqlTable table;
        private final Schema schema;
        private final String schemaUuid;
        private final GenericDatumWriter<GenericRecord> writer;
        private final GenericDatumReader<GenericRecord> reader;

        private SchemaCacheEntry(CqlTable table, Schema payloadSchema)
        {
            this.table = table;
            this.schema = payloadSchema;
            this.schemaUuid = UUID.nameUUIDFromBytes(table.createStatement().getBytes(StandardCharsets.UTF_8)).toString();
            this.writer = new GenericDatumWriter<>(payloadSchema);
            this.reader = new GenericDatumReader<>(payloadSchema);
        }

        public String tableSchema()
        {
            return table.createStatement();
        }
    }
}
