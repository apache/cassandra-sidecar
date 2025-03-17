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

package org.apache.cassandra.sidecar.datahub;

import java.util.List;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.linkedin.data.template.RecordTemplate;
import datahub.client.Emitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.metrics.DeltaGauge;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.SchemaReportingMetrics;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class for converting and reporting the provided Cassandra metadata objects
 * in a DataHub-compliant format describing the current schema of the cluster.
 * <p>
 * Note that the extensive usage of {@link Stream} types here enables late creation and
 * early destruction of DataHub aspect objects while the schema is being converted
 * (since all of them can potentially take up to a gigabyte on largest clusters).
 */
@Singleton
public class SchemaReporter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaReporter.class);

    @NotNull
    protected final IdentifiersProvider identifiersProvider;
    @NotNull
    protected final List<ClusterToAspectConverter<? extends RecordTemplate>> clusterConverters;
    @NotNull
    protected final List<KeyspaceToAspectConverter<? extends RecordTemplate>> keyspaceConverters;
    @NotNull
    protected final List<TableToAspectConverter<? extends RecordTemplate>> tableConverters;
    @NotNull
    protected final EmitterFactory emitterFactory;
    @NotNull
    protected final SchemaReportingMetrics reportingMetrics;

    /**
     * The public constructor that instantiates {@link SchemaReporter} with default configuration.
     * <p>
     * The specific combination of converters used by this constructor should be considered a part
     * of the API and should not generally be changed, as any change will break existing users.
     *
     * @param identifiersProvider an instance of {@link IdentifiersProvider} to use
     * @param emitterFactory an instance of {@link EmitterFactory} to use
     * @param sidecarMetrics an instance of {@link SidecarMetrics} to obtain {@link SchemaReportingMetrics} from
     */
    @Inject
    public SchemaReporter(@NotNull IdentifiersProvider identifiersProvider,
                          @NotNull EmitterFactory emitterFactory,
                          @NotNull SidecarMetrics sidecarMetrics)
    {
        this(identifiersProvider,
             ImmutableList.of(new ClusterToDataPlatformInfoConverter(identifiersProvider),
                              new ClusterToDataPlatformInstancePropertiesConverter(identifiersProvider)),
             ImmutableList.of(new KeyspaceToContainerPropertiesConverter(identifiersProvider),
                              new KeyspaceToSubTypesConverter(identifiersProvider),
                              new KeyspaceToDataPlatformInstanceConverter(identifiersProvider),
                              new KeyspaceToBrowsePathsV2Converter(identifiersProvider)),
             ImmutableList.of(new TableToDatasetPropertiesConverter(identifiersProvider),
                              new TableToSchemaMetadataConverter(identifiersProvider),
                              new TableToContainerConverter(identifiersProvider),
                              new TableToSubTypesConverter(identifiersProvider),
                              new TableToDataPlatformInstanceConverter(identifiersProvider),
                              new TableToBrowsePathsV2Converter(identifiersProvider),
                              new TableToBrowsePathsConverter(identifiersProvider)),
             emitterFactory,
             sidecarMetrics.server().schemaReporting());
    }

    /**
     * A protected constructor that can be used to instantiate {@link SchemaReporter} with custom configuration
     *
     * @param identifiersProvider an instance of {@link IdentifiersProvider} to use
     * @param clusterConverters a {@link List} of {@link ClusterToAspectConverter} instances to use
     * @param keyspaceConverters a {@link List} of {@link KeyspaceToAspectConverter} instances to use
     * @param tableConverters a {@link List} of {@link TableToAspectConverter} instances to use
     * @param emitterFactory an instance of {@link EmitterFactory} to use
     * @param reportingMetrics an instance of {@link SchemaReportingMetrics} to use
     */
    protected SchemaReporter(@NotNull IdentifiersProvider identifiersProvider,
                             @NotNull List<ClusterToAspectConverter<? extends RecordTemplate>> clusterConverters,
                             @NotNull List<KeyspaceToAspectConverter<? extends RecordTemplate>> keyspaceConverters,
                             @NotNull List<TableToAspectConverter<? extends RecordTemplate>> tableConverters,
                             @NotNull EmitterFactory emitterFactory,
                             @NotNull SchemaReportingMetrics reportingMetrics)
    {
        this.identifiersProvider = identifiersProvider;
        this.clusterConverters = clusterConverters;
        this.keyspaceConverters = keyspaceConverters;
        this.tableConverters = tableConverters;
        this.emitterFactory = emitterFactory;
        this.reportingMetrics = reportingMetrics;
    }

    /**
     * Public method for converting and reporting the Cassandra schema when triggered by a scheduled periodic task
     *
     * @param cluster the {@link Cluster} to extract Cassandra schema from
     */
    public void processScheduled(@NotNull Cluster cluster)
    {
        process(cluster.getMetadata(), reportingMetrics.startedSchedule.metric);
    }

    /**
     * Public method for converting and reporting the Cassandra schema when triggered by a received API request
     *
     * @param metadata the {@link Metadata} to extract Cassandra schema from
     */
    public void processRequested(@NotNull Metadata metadata)
    {
        process(metadata, reportingMetrics.startedRequest.metric);
    }

    /**
     * Private method for converting and reporting the Cassandra schema
     *
     * @param metadata the {@link Metadata} to extract Cassandra schema from
     * @param started the {@link DeltaGauge} for the metric counting invocations
     */
    private void process(@NotNull Metadata metadata,
                         @NotNull DeltaGauge started)
    {
        LOGGER.info("Starting to report schema for cluster, identifiers={}", identifiersProvider);
        started.increment();

        try (Emitter emitter = emitterFactory.emitter())
        {
            Timer.Context timer = reportingMetrics.totalDuration.metric.time();
            long aspects = stream(metadata).map(ThrowableUtils.function(emitter::emit))
                                           .count();

            timer.close();
            reportingMetrics.sizeAspects.metric.update(aspects);
            reportingMetrics.finishedSuccess.metric.increment();
            LOGGER.info("Successfully reported schema for cluster, identifiers={}", identifiersProvider);
        }
        catch (Exception exception)
        {
            reportingMetrics.finishedFailure.metric.increment();
            LOGGER.error("Failed to report schema for cluster, identifiers={}", identifiersProvider);

            throw new RuntimeException("Failed to report schema for cluster with identifiers " + identifiersProvider,
                                       exception);
        }
    }

    /**
     * Protected method that converts Cassandra cluster metadata
     * into a non-empty {@link Stream} of DataHub aspects
     *
     * @param metadata Cassandra cluster metadata
     * @return non-empty {@link Stream} of DataHub aspects
     */
    @NotNull
    protected Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> stream(@NotNull Metadata metadata)
    {
        return Streams.concat(
        clusterConverters.stream()
                         .map(ThrowableUtils.function(converter -> converter.convert(metadata))),
        metadata.getKeyspaces()
                .stream()
                .filter(this::neitherVirtualNorSystem)
                .flatMap(this::stream));
    }

    /**
     * Protected method that converts Cassandra keyspace metadata
     * into a non-empty {@link Stream} of DataHub aspects
     *
     * @param keyspace Cassandra keyspace metadata
     * @return non-empty {@link Stream} of DataHub aspects
     */
    @NotNull
    protected Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> stream(@NotNull KeyspaceMetadata keyspace)
    {
        return Streams.concat(
        keyspaceConverters.stream()
                          .map(ThrowableUtils.function(converter -> converter.convert(keyspace))),
        keyspace.getTables()
                .stream()
                .flatMap(this::stream));
    }

    /**
     * Protected method that converts Cassandra table metadata
     * into a non-empty {@link Stream} of DataHub aspects
     *
     * @param table Cassandra table metadata
     * @return non-empty {@link Stream} of DataHub aspects
     */
    @NotNull
    protected Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> stream(@NotNull TableMetadata table)
    {
        return tableConverters.stream()
                              .map(ThrowableUtils.function(converter -> converter.convert(table)));
    }

    /**
     * Protected method for filtering out virtual keyspaces,
     * Cassandra system keyspaces, and Sidecar internal keyspaces
     *
     * @param keyspace Cassandra keyspace metadata
     * @return {@code true} if the keyspace is neither virtual nor system,
     *         {@code false} otherwise
     */
    protected boolean neitherVirtualNorSystem(@NotNull KeyspaceMetadata keyspace)
    {
        if (keyspace.isVirtual())
        {
            return false;
        }

        String name = keyspace.getName();
        return !name.equals("system") &&
               !name.startsWith("system_") &&
               !name.equals("sidecar_internal");
    }
}
