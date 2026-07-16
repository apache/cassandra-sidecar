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

package org.apache.cassandra.sidecar.modules;

import java.io.IOException;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import io.vertx.core.Vertx;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.cdc.kafka.KafkaProducerFactory;
import org.apache.cassandra.cdc.schemastore.SchemaStorePublisherFactory;
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.stats.CdcStats;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.sidecar.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.cdc.CachingSchemaStore;
import org.apache.cassandra.sidecar.cdc.CdcConfig;
import org.apache.cassandra.sidecar.cdc.CdcConfigImpl;
import org.apache.cassandra.sidecar.cdc.CdcDynamicSidecarInstancesProvider;
import org.apache.cassandra.sidecar.cdc.CdcLogCache;
import org.apache.cassandra.sidecar.cdc.CdcPublisher;
import org.apache.cassandra.sidecar.cdc.CdcSchemaSupplier;
import org.apache.cassandra.sidecar.cdc.SidecarCdcOptions;
import org.apache.cassandra.sidecar.cdc.SidecarCdcStats;
import org.apache.cassandra.sidecar.cdc.SidecarClientSecretsProvider;
import org.apache.cassandra.sidecar.cdc.SidecarClusterConfigProvider;
import org.apache.cassandra.sidecar.cdc.SidecarCqlToAvroSchemaConverter;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.data.AllServicesConfigPayload;
import org.apache.cassandra.sidecar.common.response.ListCdcSegmentsResponse;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.coordination.CassandraClientTokenRingProvider;
import org.apache.cassandra.sidecar.coordination.ContentionFreeRangeManager;
import org.apache.cassandra.sidecar.coordination.InnerDcTokenAdjacentPeerProvider;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.coordination.SidecarHttpHealthProvider;
import org.apache.cassandra.sidecar.coordination.SidecarPeerHealthMonitorTask;
import org.apache.cassandra.sidecar.coordination.SidecarPeerHealthProvider;
import org.apache.cassandra.sidecar.coordination.SidecarPeerProvider;
import org.apache.cassandra.sidecar.coordination.TokenRingProvider;
import org.apache.cassandra.sidecar.db.CdcConfigAccessor;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.CdcSystemViewsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.DriverUnsupportedSchemaCache;
import org.apache.cassandra.sidecar.db.KafkaConfigAccessor;
import org.apache.cassandra.sidecar.db.TokenSplitConfigAccessor;
import org.apache.cassandra.sidecar.db.schema.CdcStatesSchema;
import org.apache.cassandra.sidecar.db.schema.ConfigsSchema;
import org.apache.cassandra.sidecar.db.schema.SystemViewsSchema;
import org.apache.cassandra.sidecar.db.schema.TableHistorySchema;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.handlers.cdc.AllServiceConfigHandler;
import org.apache.cassandra.sidecar.handlers.cdc.DeleteServiceConfigHandler;
import org.apache.cassandra.sidecar.handlers.cdc.ListCdcDirHandler;
import org.apache.cassandra.sidecar.handlers.cdc.StreamCdcSegmentHandler;
import org.apache.cassandra.sidecar.handlers.cdc.UpdateServiceConfigHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.TableSchemaMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.apache.cassandra.sidecar.tasks.CassandraClusterSchemaMonitor;
import org.apache.cassandra.sidecar.tasks.CdcConfigRefresherNotifierTask;
import org.apache.cassandra.sidecar.tasks.CdcRawDirectorySpaceCleaner;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;
import org.apache.cassandra.sidecar.utils.TokenSplitUtil;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;

/**
 * Provides Cassandra change-data capture (CDC) publishing capability
 */
@Path("/")
public class CdcModule extends AbstractModule
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcModule.class);

    @Override
    protected void configure()
    {
        bind(CachingSchemaStore.class).in(Scopes.SINGLETON);
        bind(CdcRawDirectorySpaceCleaner.class).in(Scopes.SINGLETON);
        bind(SidecarPeerHealthMonitorTask.class).in(Scopes.SINGLETON);
        bind(InnerDcTokenAdjacentPeerProvider.class).in(Scopes.SINGLETON);
        bind(CdcDatabaseAccessor.class).in(Scopes.SINGLETON);
        bind(CdcConfigAccessor.class).in(Scopes.SINGLETON);
        bind(KafkaConfigAccessor.class).in(Scopes.SINGLETON);
        bind(TokenSplitConfigAccessor.class).in(Scopes.SINGLETON);
        bind(CdcSystemViewsDatabaseAccessor.class).in(Scopes.SINGLETON);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.SidecarPeerHealthMonitorTaskKey.class)
    PeriodicTask sidecarPeerHealthMonitorTask(SidecarPeerHealthMonitorTask task)
    {
        // Wire SidecarPeerHealthMonitorTask singleton into mapBinder
        return task;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.CdcRawDirectorySpaceCleanerTaskKey.class)
    PeriodicTask cdcRawDirectorySpaceCleanercPeriodicTask(CdcRawDirectorySpaceCleaner cleanerTask)
    {
        return cleanerTask;
    }

    @Provides
    @Singleton
    CassandraClusterSchemaMonitor cassandraClusterSchemaMonitorInstance(InstanceMetadataFetcher instanceMetadataFetcher,
                                                                        CdcDatabaseAccessor databaseAccessor,
                                                                        DriverUnsupportedSchemaCache driverUnsupportedSchemaCache,
                                                                        SidecarConfiguration configuration,
                                                                        CassandraBridgeFactory cassandraBridgeFactory)
    {
        return new CassandraClusterSchemaMonitor(instanceMetadataFetcher, databaseAccessor,
                                                 driverUnsupportedSchemaCache, configuration,
                                                 cassandraBridgeFactory);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.CassandraClusterSchemaTaskKey.class)
    PeriodicTask cassandraClusterSchemaMonitor(CassandraClusterSchemaMonitor monitor)
    {
        // Wire the singleton instance into the periodic tasks map
        return monitor;
    }

    @Provides
    @Singleton
    CqlToAvroSchemaConverter cqlToAvroSchemaConverter(InstanceMetadataFetcher instanceMetadataFetcher,
                                                      CassandraBridgeFactory cassandraBridgeFactory)
    {

        return new SidecarCqlToAvroSchemaConverter(instanceMetadataFetcher, cassandraBridgeFactory);
    }

    @Provides
    @Singleton
    CdcConfig cdcConfig(Vertx vertx,
                        CdcConfigAccessor cdcConfigAccessor)
    {
        return new CdcConfigImpl(vertx, cdcConfigAccessor);
    }

    @Provides
    @Singleton
    CassandraBridgeFactory cassandraBridgeFactory()
    {
        return new CassandraBridgeFactory();
    }

    @Provides
    @Singleton
    SchemaStorePublisherFactory schemaStorePublisherFactory()
    {
        return SchemaStorePublisherFactory.DEFAULT;
    }

    @Provides
    @Singleton
    TokenSplitUtil tokenSplitUtilProvider(TokenSplitConfigAccessor tokenSplitConfigAccessor,
                                          CdcConfig cdcConfig,
                                          InstanceMetadataFetcher fetcher)
    {
        return new TokenSplitUtil(tokenSplitConfigAccessor, cdcConfig, fetcher);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.ConfigsSchemaKey.class)
    TableSchema configsSchema(ServiceConfiguration serviceConfiguration)
    {
        return new ConfigsSchema(serviceConfiguration);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.SystemViewsSchemaKey.class)
    TableSchema systemViewsSchema(SystemViewsSchema schema)
    {
        return schema;
    }

    @GET
    @Path(ApiEndpointsV1.LIST_CDC_SEGMENTS_ROUTE)
    @Operation(summary = "List CDC segments",
               description = "Lists CDC (Change Data Capture) segments available for streaming")
    @APIResponse(description = "CDC segments listed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = ListCdcSegmentsResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListCdcSegmentsRouteKey.class)
    VertxRoute listCdcSegmentsRoute(RouteBuilder.Factory factory,
                                    ListCdcDirHandler listCdcDirHandler)
    {
        return factory.buildRouteWithHandler(listCdcDirHandler);
    }

    @GET
    @Path(ApiEndpointsV1.STREAM_CDC_SEGMENTS_ROUTE)
    @Operation(summary = "Stream CDC segment",
               description = "Streams a specific CDC segment file for consumption")
    @APIResponse(description = "CDC segment stream initiated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/octet-stream",
                 schema = @Schema(type = SchemaType.STRING)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.StreamCdcSegmentRouteKey.class)
    VertxRoute streamCdcSegmentRoute(RouteBuilder.Factory factory,
                                     StreamCdcSegmentHandler streamCdcSegmentHandler)
    {
        return factory.buildRouteWithHandler(streamCdcSegmentHandler);
    }

    @GET
    @Path(ApiEndpointsV1.SERVICES_CONFIG_ROUTE)
    @Operation(summary = "Get all service configurations",
               description = "Returns all service configuration settings")
    @APIResponse(description = "Service configurations retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = AllServicesConfigPayload.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.GetAllServiceConfigurationsRouteKey.class)
    VertxRoute getAllServiceConfigurationsRoute(RouteBuilder.Factory factory,
                                                AllServiceConfigHandler allServiceConfigHandler)
    {
        return factory.buildRouteWithHandler(allServiceConfigHandler);
    }

    @PUT
    @Path(ApiEndpointsV1.SERVICE_CONFIG_ROUTE)
    @Operation(summary = "Update service configuration",
               description = "Updates service configuration settings")
    @APIResponse(description = "Service configuration updated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.UpdateServiceConfigurationRouteKey.class)
    VertxRoute updateServiceConfigurationRoute(RouteBuilder.Factory factory,
                                               UpdateServiceConfigHandler updateServiceConfigHandler)
    {
        return factory.builderForRoute().setBodyHandler(true).handler(updateServiceConfigHandler).build();
    }

    @DELETE
    @Path(ApiEndpointsV1.SERVICE_CONFIG_ROUTE)
    @Operation(summary = "Delete service configuration",
               description = "Deletes a specific service configuration setting")
    @APIResponse(description = "Service configuration deleted successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @APIResponse(responseCode = "404",
                 description = "Configuration key not found",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeleteServiceConfigurationRouteKey.class)
    VertxRoute deleteServiceConfigurationRoute(RouteBuilder.Factory factory,
                                               DeleteServiceConfigHandler deleteServiceConfigHandler)
    {
        return factory.buildRouteWithHandler(deleteServiceConfigHandler);
    }

    @Provides
    @Singleton
    CdcLogCache cdcLogCache(ExecutorPools executorPools,
                            InstancesMetadata instancesMetadata,
                            SidecarConfiguration sidecarConfig)
    {
        return new CdcLogCache(executorPools, instancesMetadata, sidecarConfig);
    }

    @Provides
    @Singleton
    public SidecarPeerHealthProvider sidecarHealthProvider(SidecarClientProvider sidecarClientProvider)
    {
        return new SidecarHttpHealthProvider(sidecarClientProvider);
    }

    @Provides
    @Singleton
    public SidecarPeerProvider sidecarPeerProvider(InnerDcTokenAdjacentPeerProvider innerDcTokenAdjacentPeerProvider)
    {
        return innerDcTokenAdjacentPeerProvider;
    }

    @Provides
    @Singleton
    public CdcSidecarInstancesProvider cdcSidecarInstancesProvider(InstancesMetadata instancesMetadata, ServiceConfiguration serviceConfiguration)
    {
        return new CdcDynamicSidecarInstancesProvider(instancesMetadata, serviceConfiguration);
    }

    @Provides
    @Singleton
    public SidecarCdcStats sidecarCdcStats()
    {
        return new SidecarCdcStats()
        {
        };
    }

    @Provides
    @Singleton
    public KafkaProducerFactory kafkaProducerFactory()
    {
        return KafkaProducerFactory.DEFAULT;
    }

    @Provides
    @Singleton
    public SchemaSupplier schemaSupplier(InstanceMetadataFetcher instanceMetadataFetcher,
                                         CassandraBridgeFactory cassandraBridgeFactory,
                                         CdcDatabaseAccessor cdcDatabaseAccessor)
    {
        return new CdcSchemaSupplier(instanceMetadataFetcher, cassandraBridgeFactory, cdcDatabaseAccessor);
    }

    @Provides
    @Singleton
    public ClusterConfigProvider clusterConfigProvider(InstanceMetadataFetcher instanceMetadataFetcher)
    {
        return new SidecarClusterConfigProvider(instanceMetadataFetcher);
    }

    @Provides
    @Singleton
    public ICdcStats cdcStats()
    {
        return new CdcStats()
        {
        };
    }

    @Provides
    @Singleton
    public TokenRingProvider tokenRingProvider(InstancesMetadata instancesMetadata, InstanceMetadataFetcher instanceMetadataFetcher, DnsResolver dnsResolver)
    {
        return new CassandraClientTokenRingProvider(instancesMetadata, instanceMetadataFetcher, dnsResolver);
    }

    @Provides
    @Singleton
    public SidecarCdcClient.ClientConfig clientConfig(SidecarConfiguration sidecarConfiguration)
    {
        SidecarClientConfiguration sidecarClientConfiguration = sidecarConfiguration.sidecarClientConfiguration();
        return SidecarCdcClient.ClientConfig.create(sidecarConfiguration.serviceConfiguration().port(),
                                                    sidecarClientConfiguration.maxRetries(),
                                                    sidecarClientConfiguration.retryDelay().toIntMillis());
    }

    @Provides
    @Singleton
    public SidecarCdcClient sidecarCdcClient(Vertx vertx,
                                             SidecarCdcClient.ClientConfig clientConfig,
                                             CdcSidecarInstancesProvider cdcSidecarInstancesProvider,
                                             @Nullable SecretsProvider secretsProvider,
                                             ICdcStats cdcStats)
    {
        try
        {
            SidecarCdcClient sidecarCdcClient = new SidecarCdcClient(clientConfig, cdcSidecarInstancesProvider, secretsProvider, cdcStats);
            vertx.eventBus().localConsumer(ON_SERVER_STOP.address(), message -> {
                try
                {
                    sidecarCdcClient.close();
                }
                catch (Exception e)
                {
                    LOGGER.warn("Error closing SidecarCdcClient", e);
                }
            });
            return sidecarCdcClient;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to create SidecarCdcClient", e);
        }
    }

    @Nullable
    @Provides
    @Singleton
    public SecretsProvider secretsProvider(SidecarConfiguration sidecarConfiguration)
    {
        SslConfiguration sslConfiguration = sidecarConfiguration.sidecarClientConfiguration().sslConfiguration();

        if (sslConfiguration == null || !sslConfiguration.enabled())
        {
            return null;
        }

        return new SidecarClientSecretsProvider(sidecarConfiguration);
    }

    @Provides
    @Singleton
    RangeManager rangeManager(Vertx vertx, TokenRingProvider tokenRingProvider)
    {
        return new ContentionFreeRangeManager(vertx, tokenRingProvider);
    }

    @Provides
    @Singleton
    CdcPublisher cdcPublisher(Vertx vertx,
                              ExecutorPools executorPools,
                              ClusterConfigProvider clusterConfigProvider,
                              SchemaSupplier schemaSupplier,
                              InstanceMetadataFetcher instanceMetadataFetcher,
                              CdcConfig conf,
                              CdcDatabaseAccessor databaseAccessor,
                              ICdcStats cdcStats,
                              CdcSystemViewsDatabaseAccessor systemViews,
                              SidecarCdcStats sidecarCdcStats,
                              RangeManager rangeManager,
                              CassandraBridgeFactory cassandraBridgeFactory,
                              Provider<SidecarCdcClient> sidecarCdcClientProvider,
                              CachingSchemaStore schemaStore,
                              KafkaProducerFactory kafkaProducerFactory,
                              CdcOptions cdcOptions)
    {
        return new CdcPublisher(vertx,
                                executorPools,
                                clusterConfigProvider,
                                schemaSupplier,
                                instanceMetadataFetcher,
                                conf,
                                databaseAccessor,
                                cdcStats,
                                systemViews,
                                sidecarCdcStats,
                                () -> rangeManager,
                                cassandraBridgeFactory,
                                sidecarCdcClientProvider,
                                schemaStore,
                                kafkaProducerFactory,
                                cdcOptions);
    }

    @Provides
    @Singleton
    public CdcOptions cdcOptions(InstanceMetadataFetcher instanceMetadataFetcher, CdcConfig conf)
    {
        return new SidecarCdcOptions(instanceMetadataFetcher, conf);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.CdcPublisherTaskKey.class)
    PeriodicTask cdcPublisherTask(CdcPublisher cdcPublisher)
    {
        return cdcPublisher;
    }

    @Singleton
    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.CdcConfigRefresherNotifierKey.class)
    PeriodicTask cdcConfigRefresherNotifier(Vertx vertx,
                                            SidecarConfiguration sidecarConfiguration,
                                            KafkaConfigAccessor kafkaConfigAccessor,
                                            CdcConfigAccessor cdcConfigAccessor)
    {
        return new CdcConfigRefresherNotifierTask(vertx,
                                                  sidecarConfiguration,
                                                  kafkaConfigAccessor,
                                                  cdcConfigAccessor);
    }

    @Singleton
    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.TableHistorySchemaKey.class)
    TableSchema tableHistorySchema(SidecarConfiguration configuration)
    {
        return new TableHistorySchema(configuration.serviceConfiguration());
    }

    @Singleton
    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.CdcStatesSchemaKey.class)
    TableSchema cdcStatesSchema(SidecarConfiguration configuration)
    {
        return new CdcStatesSchema(configuration.serviceConfiguration());
    }
}
