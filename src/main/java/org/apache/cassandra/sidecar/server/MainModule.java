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

package org.apache.cassandra.sidecar.server;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.NettyOptions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.mtls.AllowAllCertificateValidator;
import io.vertx.ext.auth.mtls.AllowAllIdentityValidator;
import io.vertx.ext.auth.mtls.MutualTlsAuthenticationProvider;
import io.vertx.ext.auth.mtls.MutualTlsCertificateValidator;
import io.vertx.ext.auth.mtls.MutualTlsIdentityValidator;
import io.vertx.ext.auth.mtls.MutualTlsIdentityValidatorImpl;
import io.vertx.ext.auth.mtls.RejectAllCertificateValidator;
import io.vertx.ext.auth.mtls.RejectAllIdentityValidator;
import io.vertx.ext.auth.mtls.SpiffeCertificateValidator;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.dropwizard.Match;
import io.vertx.ext.dropwizard.MatchType;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.AuthorizationHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.adapters.cassandra41.Cassandra41Factory;
import org.apache.cassandra.sidecar.auth.authentication.AllowAllAuthenticationProvider;
import org.apache.cassandra.sidecar.auth.authentication.AuthenticatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.CertificateValidatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.IdentityValidatorConfig;
import org.apache.cassandra.sidecar.auth.authorization.AllowAllAuthorizationProvider;
import org.apache.cassandra.sidecar.auth.authorization.AuthorizerConfig;
import org.apache.cassandra.sidecar.auth.authorization.MutualTlsAuthorizationProvider;
import org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions;
import org.apache.cassandra.sidecar.auth.authorization.PermissionsAccessor;
import org.apache.cassandra.sidecar.auth.authorization.RequiredPermissionsProvider;
import org.apache.cassandra.sidecar.auth.authorization.SystemAuthSchema;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.cluster.locator.CachedLocalTokenRanges;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.server.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.VertxMetricsConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.db.schema.RestoreJobsSchema;
import org.apache.cassandra.sidecar.db.schema.RestoreRangesSchema;
import org.apache.cassandra.sidecar.db.schema.RestoreSlicesSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarInternalKeyspace;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.logging.SidecarLoggerHandler;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SchemaMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.metrics.instance.InstanceHealthMetrics;
import org.apache.cassandra.sidecar.routes.CassandraHealthHandler;
import org.apache.cassandra.sidecar.routes.DiskSpaceProtectionHandler;
import org.apache.cassandra.sidecar.routes.FileStreamHandler;
import org.apache.cassandra.sidecar.routes.GossipInfoHandler;
import org.apache.cassandra.sidecar.routes.JsonErrorHandler;
import org.apache.cassandra.sidecar.routes.MutualTlsAuthenticationHandler;
import org.apache.cassandra.sidecar.routes.MutualTlsAuthorizationHandler;
import org.apache.cassandra.sidecar.routes.RingHandler;
import org.apache.cassandra.sidecar.routes.RoutingOrder;
import org.apache.cassandra.sidecar.routes.SchemaHandler;
import org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.routes.TimeSkewHandler;
import org.apache.cassandra.sidecar.routes.TokenRangeReplicaMapHandler;
import org.apache.cassandra.sidecar.routes.cassandra.NodeSettingsHandler;
import org.apache.cassandra.sidecar.routes.restore.AbortRestoreJobHandler;
import org.apache.cassandra.sidecar.routes.restore.CreateRestoreJobHandler;
import org.apache.cassandra.sidecar.routes.restore.CreateRestoreSliceHandler;
import org.apache.cassandra.sidecar.routes.restore.RestoreJobProgressHandler;
import org.apache.cassandra.sidecar.routes.restore.RestoreJobSummaryHandler;
import org.apache.cassandra.sidecar.routes.restore.RestoreRequestValidationHandler;
import org.apache.cassandra.sidecar.routes.restore.UpdateRestoreJobHandler;
import org.apache.cassandra.sidecar.routes.snapshots.ClearSnapshotHandler;
import org.apache.cassandra.sidecar.routes.snapshots.CreateSnapshotHandler;
import org.apache.cassandra.sidecar.routes.snapshots.ListSnapshotHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableCleanupHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableImportHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableUploadHandler;
import org.apache.cassandra.sidecar.routes.validations.ValidateTableExistenceHandler;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.DigestAlgorithmProvider;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.JdkMd5DigestProvider;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.apache.cassandra.sidecar.utils.XXHash32Provider;

import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.ABORT_RESTORE_JOB;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.CLEANUP_SSTABLE;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.CLEAR_SNAPSHOTS;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.CREATE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.CREATE_SNAPSHOT;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.GOSSIP_INFO;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.KEYSPACE_SCHEMA;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.KEYSPACE_TOKEN_MAPPING;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.LIST_SNAPSHOTS;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.PATCH_RESTORE_JOB;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.RESTORE_JOB;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.RESTORE_JOB_PROGRESS;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.RING;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.STREAM_SSTABLES;
import static org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions.UPLOAD_SSTABLE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.API_V1_ALL_ROUTES;
import static org.apache.cassandra.sidecar.common.server.utils.ByteUtils.bytesToHumanReadableBinaryPrefix;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;

/**
 * Provides main binding for more complex Guice dependencies
 */
public class MainModule extends AbstractModule
{
    public static final Map<String, String> OK_STATUS = Collections.singletonMap("status", "OK");
    public static final Map<String, String> NOT_OK_STATUS = Collections.singletonMap("status", "NOT_OK");
    private static final Logger LOGGER = LoggerFactory.getLogger(MainModule.class);
    protected final Path confPath;

    /**
     * Constructs the Guice main module to run Cassandra Sidecar
     */
    public MainModule()
    {
        confPath = null;
    }

    /**
     * Constructs the Guice main module with the configured yaml {@code confPath} to run Cassandra Sidecar
     *
     * @param confPath the path to the yaml configuration file
     */
    public MainModule(Path confPath)
    {
        this.confPath = confPath;
    }

    /**
     * Builds the {@link InstanceMetadata} from the {@link InstanceConfiguration},
     * a provided {@code  versionProvider}, and {@code healthCheckFrequencyMillis}.
     *
     * @param vertx             the vertx instance
     * @param cassandraInstance the cassandra instance configuration
     * @param versionProvider   a Cassandra version provider
     * @param sidecarVersion    the version of the Sidecar from the current binary
     * @param jmxConfiguration  the configuration for the JMX Client
     * @param session           the CQL Session provider
     * @param registryFactory   factory for creating cassandra instance specific registry
     * @return the build instance metadata object
     */
    private static InstanceMetadata buildInstanceMetadata(Vertx vertx,
                                                          InstanceConfiguration cassandraInstance,
                                                          CassandraVersionProvider versionProvider,
                                                          String sidecarVersion,
                                                          JmxConfiguration jmxConfiguration,
                                                          CQLSessionProvider session,
                                                          DriverUtils driverUtils,
                                                          MetricRegistryFactory registryFactory)
    {
        String host = cassandraInstance.host();
        int port = cassandraInstance.port();

        JmxClient jmxClient = JmxClient.builder()
                                       .host(cassandraInstance.jmxHost())
                                       .port(cassandraInstance.jmxPort())
                                       .role(cassandraInstance.jmxRole())
                                       .password(cassandraInstance.jmxRolePassword())
                                       .enableSsl(cassandraInstance.jmxSslEnabled())
                                       .connectionMaxRetries(jmxConfiguration.maxRetries())
                                       .connectionRetryDelayMillis(jmxConfiguration.retryDelayMillis())
                                       .build();
        MetricRegistry instanceSpecificRegistry = registryFactory.getOrCreate(cassandraInstance.id());
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
                                                                         cassandraInstance.id(),
                                                                         versionProvider,
                                                                         session,
                                                                         jmxClient,
                                                                         driverUtils,
                                                                         sidecarVersion,
                                                                         host,
                                                                         port,
                                                                         new InstanceHealthMetrics(instanceSpecificRegistry));
        return InstanceMetadataImpl.builder()
                                   .id(cassandraInstance.id())
                                   .host(host)
                                   .port(port)
                                   .dataDirs(cassandraInstance.dataDirs())
                                   .stagingDir(cassandraInstance.stagingDir())
                                   .delegate(delegate)
                                   .metricRegistry(instanceSpecificRegistry)
                                   .build();
    }

    @Provides
    @Singleton
    public Vertx vertx(SidecarConfiguration sidecarConfiguration, MetricRegistryFactory metricRegistryFactory)
    {
        VertxMetricsConfiguration metricsConfig = sidecarConfiguration.metricsConfiguration().vertxConfiguration();
        Match serverUriMatch = new Match().setValue(API_V1_ALL_ROUTES).setType(MatchType.REGEX);
        DropwizardMetricsOptions dropwizardMetricsOptions
        = new DropwizardMetricsOptions().setEnabled(metricsConfig.enabled())
                                        .setJmxEnabled(metricsConfig.exposeViaJMX())
                                        .setJmxDomain(metricsConfig.jmxDomainName())
                                        .setMetricRegistry(metricRegistryFactory.getOrCreate())
                                        // Monitor all V1 endpoints.
                                        // Additional filtering is done by configuring yaml fields 'metrics.include|exclude'
                                        .addMonitoredHttpServerUri(serverUriMatch);
        return Vertx.vertx(new VertxOptions().setMetricsOptions(dropwizardMetricsOptions));
    }

    @Provides
    @Singleton
    public Router vertxRouter(Vertx vertx,
                              AuthenticationHandler authenticationHandler,
                              PermissionsAccessor permissionsAccessor,
                              RequiredPermissionsProvider requiredPermissionsProvider,
                              SidecarConfiguration sidecarConfiguration,
                              ServiceConfiguration conf,
                              CassandraHealthHandler cassandraHealthHandler,
                              StreamSSTableComponentHandler streamSSTableComponentHandler,
                              FileStreamHandler fileStreamHandler,
                              ClearSnapshotHandler clearSnapshotHandler,
                              CreateSnapshotHandler createSnapshotHandler,
                              ListSnapshotHandler listSnapshotHandler,
                              SchemaHandler schemaHandler,
                              RingHandler ringHandler,
                              TokenRangeReplicaMapHandler tokenRangeHandler,
                              LoggerHandler loggerHandler,
                              GossipInfoHandler gossipInfoHandler,
                              TimeSkewHandler timeSkewHandler,
                              NodeSettingsHandler nodeSettingsHandler,
                              SSTableUploadHandler ssTableUploadHandler,
                              SSTableImportHandler ssTableImportHandler,
                              SSTableCleanupHandler ssTableCleanupHandler,
                              RestoreRequestValidationHandler validateRestoreJobRequest,
                              DiskSpaceProtectionHandler diskSpaceProtection,
                              ValidateTableExistenceHandler validateTableExistence,
                              CreateRestoreJobHandler createRestoreJobHandler,
                              RestoreJobSummaryHandler restoreJobSummaryHandler,
                              UpdateRestoreJobHandler updateRestoreJobHandler,
                              AbortRestoreJobHandler abortRestoreJobHandler,
                              CreateRestoreSliceHandler createRestoreSliceHandler,
                              RestoreJobProgressHandler restoreJobProgressHandler,
                              ErrorHandler errorHandler)
    {
        Router router = Router.router(vertx);

        router.route()
              .order(RoutingOrder.HIGHEST.order)
              .handler(loggerHandler)
              .handler(TimeoutHandler.create(conf.requestTimeoutMillis(),
                                             HttpResponseStatus.REQUEST_TIMEOUT.code()));

        router.route()
              .path(ApiEndpointsV1.API + "/*")
              .order(RoutingOrder.HIGHEST.order)
              .handler(authenticationHandler)
              .failureHandler(errorHandler);

        // Docs index.html page
        StaticHandler docs = StaticHandler.create("docs");
        router.route()
              .path("/docs/*")
              .handler(docs);

        // Add custom routers
        // Provides a simple REST endpoint to determine if Sidecar is available
        router.get(ApiEndpointsV1.HEALTH_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/__health",
                                            new ArrayList<>()))
              .handler(context -> context.json(OK_STATUS));

        // Backwards compatibility for the Cassandra health endpoint
        //noinspection deprecation
        router.get(ApiEndpointsV1.CASSANDRA_HEALTH_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/__health",
                                            new ArrayList<>()))
              .handler(cassandraHealthHandler);

        router.get(ApiEndpointsV1.CASSANDRA_NATIVE_HEALTH_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/native/__health",
                                            new ArrayList<>()))
              .handler(cassandraHealthHandler);

        router.get(ApiEndpointsV1.CASSANDRA_JMX_HEALTH_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/jmx/__health",
                                            new ArrayList<>()))
              .handler(cassandraHealthHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_COMPONENTS_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspace/:keyspace/table/:table/snapshots/:snapshot/component/:component",
                                            Arrays.asList(STREAM_SSTABLES)))
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        router.get(ApiEndpointsV1.COMPONENTS_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspaces/:keyspace/tables/:table/snapshots/:snapshot/components/:component",
                                            Arrays.asList(STREAM_SSTABLES)))
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        // Support for routes that want to stream SStable index components
        router.get(ApiEndpointsV1.COMPONENTS_WITH_SECONDARY_INDEX_ROUTE_SUPPORT)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspaces/:keyspace/tables/:table/snapshots/:snapshot/components/:index/:component",
                                            Arrays.asList(STREAM_SSTABLES)))
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_SNAPSHOTS_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspace/:keyspace/table/:table/snapshots/:snapshot",
                                            Arrays.asList(LIST_SNAPSHOTS)))
              .handler(listSnapshotHandler);

        router.get(ApiEndpointsV1.SNAPSHOTS_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspaces/:keyspace/tables/:table/snapshots/:snapshot",
                                            Arrays.asList(LIST_SNAPSHOTS)))
              .handler(listSnapshotHandler);

        router.delete(ApiEndpointsV1.SNAPSHOTS_ROUTE)
              // Leverage the validateTableExistence. Currently, JMX does not validate for non-existent keyspace.
              // Additionally, the current JMX implementation to clear snapshots does not support passing a table
              // as a parameter.
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "DELETE /api/v1/keyspaces/:keyspace/tables/:table/snapshots/:snapshot",
                                            Arrays.asList(CLEAR_SNAPSHOTS)))
              .handler(validateTableExistence)
              .handler(clearSnapshotHandler);

        router.put(ApiEndpointsV1.SNAPSHOTS_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "PUT /api/v1/keyspaces/:keyspace/tables/:table/snapshots/:snapshot",
                                            Arrays.asList(CREATE_SNAPSHOT)))
              .handler(createSnapshotHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_ALL_KEYSPACES_SCHEMA_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/schema/keyspaces",
                                            Arrays.asList(KEYSPACE_SCHEMA)))
              .handler(schemaHandler);

        router.get(ApiEndpointsV1.ALL_KEYSPACES_SCHEMA_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/schema",
                                            Arrays.asList(KEYSPACE_SCHEMA)))
              .handler(schemaHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_KEYSPACE_SCHEMA_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/schema/keyspaces/:keyspace",
                                            Arrays.asList(KEYSPACE_SCHEMA)))
              .handler(schemaHandler);

        router.get(ApiEndpointsV1.KEYSPACE_SCHEMA_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspaces/:keyspace/schema",
                                            Arrays.asList(KEYSPACE_SCHEMA)))
              .handler(schemaHandler);

        router.get(ApiEndpointsV1.RING_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/ring",
                                            Arrays.asList(RING)))
              .handler(ringHandler);

        router.get(ApiEndpointsV1.RING_ROUTE_PER_KEYSPACE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/ring/keyspaces/:keyspace",
                                            Arrays.asList(RING)))
              .handler(ringHandler);

        router.put(ApiEndpointsV1.SSTABLE_UPLOAD_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "PUT /api/v1/uploads/:uploadId/keyspaces/:keyspace/tables/:table/components/:component",
                                            Arrays.asList(UPLOAD_SSTABLE)))
              .handler(ssTableUploadHandler);

        router.get(ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspaces/:keyspace/token-range-replicas",
                                            Arrays.asList(KEYSPACE_TOKEN_MAPPING)))
              .handler(tokenRangeHandler);

        router.put(ApiEndpointsV1.SSTABLE_IMPORT_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "PUT /api/v1/uploads/:uploadId/keyspaces/:keyspace/tables/:table/import",
                                            Arrays.asList(UPLOAD_SSTABLE)))
              .handler(ssTableImportHandler);

        router.delete(ApiEndpointsV1.SSTABLE_CLEANUP_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "DELETE /api/v1/uploads/:uploadId",
                                            Arrays.asList(CLEANUP_SSTABLE)))
              .handler(ssTableCleanupHandler);

        router.get(ApiEndpointsV1.GOSSIP_INFO_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/gossip",
                                            Arrays.asList(GOSSIP_INFO)))
              .handler(gossipInfoHandler);

        router.get(ApiEndpointsV1.TIME_SKEW_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/time-skew",
                                            Arrays.asList()))
              .handler(timeSkewHandler);

        router.get(ApiEndpointsV1.NODE_SETTINGS_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/cassandra/settings",
                                            Arrays.asList()))
              .handler(nodeSettingsHandler);

        router.post(ApiEndpointsV1.CREATE_RESTORE_JOB_ROUTE)
              .handler(BodyHandler.create())
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "POST /api/v1/keyspaces/:keyspace/tables/:table/restore-jobs",
                                            Arrays.asList(CREATE_RESTORE_JOB)))
              .handler(validateTableExistence)
              .handler(validateRestoreJobRequest)
              .handler(createRestoreJobHandler);

        router.post(ApiEndpointsV1.RESTORE_JOB_SLICES_ROUTE)
              .handler(BodyHandler.create())
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "POST /api/v1/keyspaces/:keyspace/tables/:table/restore-jobs/:jobId/slices",
                                            Arrays.asList(CREATE_RESTORE_JOB)))
              .handler(diskSpaceProtection) // reject creating slice if short of disk space
              .handler(validateTableExistence)
              .handler(validateRestoreJobRequest)
              .handler(createRestoreSliceHandler);

        router.get(ApiEndpointsV1.RESTORE_JOB_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspaces/:keyspace/tables/:table/restore-jobs/:jobId",
                                            Arrays.asList(RESTORE_JOB)))
              .handler(validateTableExistence)
              .handler(validateRestoreJobRequest)
              .handler(restoreJobSummaryHandler);

        router.patch(ApiEndpointsV1.RESTORE_JOB_ROUTE)
              .handler(BodyHandler.create())
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "PATCH /api/v1/keyspaces/:keyspace/tables/:table/restore-jobs/:jobId",
                                            Arrays.asList(PATCH_RESTORE_JOB)))
              .handler(validateTableExistence)
              .handler(validateRestoreJobRequest)
              .handler(updateRestoreJobHandler);

        router.post(ApiEndpointsV1.ABORT_RESTORE_JOB_ROUTE)
              .handler(BodyHandler.create())
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "POST /api/v1/keyspaces/:keyspace/tables/:table/restore-jobs/:jobId/abort",
                                            Arrays.asList(ABORT_RESTORE_JOB)))
              .handler(validateTableExistence)
              .handler(validateRestoreJobRequest)
              .handler(abortRestoreJobHandler);

        router.get(ApiEndpointsV1.RESTORE_JOB_PROGRESS_ROUTE)
              .handler(authorizationHandler(sidecarConfiguration,
                                            permissionsAccessor,
                                            requiredPermissionsProvider,
                                            "GET /api/v1/keyspaces/:keyspace/tables/:table/restore-jobs/:jobId/ progress",
                                            Arrays.asList(RESTORE_JOB_PROGRESS)))
              .handler(validateTableExistence)
              .handler(validateRestoreJobRequest)
              .handler(restoreJobProgressHandler);

        return router;
    }

    public AuthorizationHandler authorizationHandler(SidecarConfiguration conf,
                                                     PermissionsAccessor permissionsAccessor,
                                                     RequiredPermissionsProvider requiredPermissionsProvider,
                                                     String endpoint,
                                                     List<MutualTlsPermissions> permissions)
    {
        requiredPermissionsProvider.putPermissionsMapping(endpoint, permissions);

        AuthorizationProvider authProvider;
        if (conf.authenticatorConfiguration() != null &&
            conf.authenticatorConfiguration().authConfig() != null &&
            conf.authorizerConfiguration().authConfig().equals(AuthorizerConfig.MutualTlsAuthorizer))
        {
            authProvider = new MutualTlsAuthorizationProvider(permissionsAccessor);
        }
        else if (conf.authenticatorConfiguration() != null &&
                 conf.authenticatorConfiguration().authConfig() != null &&
                 conf.authorizerConfiguration().authConfig().equals(AuthorizerConfig.AllowAllAuthorizer))
        {
            authProvider = new AllowAllAuthorizationProvider();
        }
        else
        {
            LOGGER.warn("Incorrect configuration - Using AllowAllAuthorizationProvider as default");
            authProvider = new AllowAllAuthorizationProvider();
        }

        return (new MutualTlsAuthorizationHandler(requiredPermissionsProvider)).addAuthorizationProvider(authProvider);
    }

    @Provides
    @Singleton
    public PermissionsAccessor permissionsAccessor(CacheFactory cacheFactory)
    {
        return new PermissionsAccessor(cacheFactory);
    }

    @Provides
    @Singleton
    public RequiredPermissionsProvider requiredPermissionsProvider()
    {
        return new RequiredPermissionsProvider();
    }

    @Provides
    @Singleton
    public MutualTlsCertificateValidator certificateValidator(SidecarConfiguration conf)
    {
        if (conf != null && conf.authenticatorConfiguration() != null && conf.authenticatorConfiguration().certValidator() != null)
        {
            if (conf.authenticatorConfiguration().certValidator().equals(CertificateValidatorConfig.SpiffeCertificateValidator))
            {
                return new SpiffeCertificateValidator();
            }
            else if (conf.authenticatorConfiguration().certValidator().equals(CertificateValidatorConfig.RejectAllCertificateValidator))
            {
                return new RejectAllCertificateValidator();
            }
            else
            {
                return new AllowAllCertificateValidator();
            }
        }
        LOGGER.warn("No valid certificate validator was listed - Defaulting to the AllowAllCertificateValidator");
        return new AllowAllCertificateValidator();
    }

    @Provides
    @Singleton
    public MutualTlsIdentityValidator identityValidator(SidecarConfiguration conf,
                                                        PermissionsAccessor permissionsAccessor)
    {
        if (conf != null && conf.authenticatorConfiguration() != null && conf.authenticatorConfiguration().idValidator() != null)
        {
            if (conf.authenticatorConfiguration().idValidator().equals(IdentityValidatorConfig.MutualTlsIdentityValidator))
            {
                return new MutualTlsIdentityValidatorImpl(s -> ((permissionsAccessor.getRoleFromIdentity(s) != null) ||
                                                                (conf != null &&
                                                                 conf.authenticatorConfiguration() != null &&
                                                                 conf.authenticatorConfiguration().authorizedIdentities() != null &&
                                                                 conf.authenticatorConfiguration().authorizedIdentities().contains(s))));
            }
            else if (conf.authenticatorConfiguration().idValidator().equals(IdentityValidatorConfig.RejectAllIdentityValidator))
            {
                return new RejectAllIdentityValidator();
            }
            else
            {
                return new AllowAllIdentityValidator();
            }
        }
        LOGGER.warn("No valid identity validator was listed - Defaulting to the AllowAllIdentityValidator");
        return new AllowAllIdentityValidator();
    }

    @Provides
    @Singleton
    public AuthenticationProvider authenticationProvider(MutualTlsCertificateValidator mTlsCertificateValidator,
                                                         MutualTlsIdentityValidator identityValidator,
                                                         SidecarConfiguration conf)
    {
        if (conf != null &&
            conf.authenticatorConfiguration() != null &&
            conf.authenticatorConfiguration().authConfig() != null &&
            conf.authenticatorConfiguration().authConfig().equals(AuthenticatorConfig.MutualTlsAuthenticator))
        {
            return new MutualTlsAuthenticationProvider(mTlsCertificateValidator, identityValidator);
        }
        else if (conf != null &&
                 conf.authenticatorConfiguration() != null &&
                 conf.authenticatorConfiguration().authConfig() != null &&
                 conf.authenticatorConfiguration().authConfig().equals(AuthenticatorConfig.AllowAllAuthenticator))
        {
            return new AllowAllAuthenticationProvider();
        }
        LOGGER.warn("Incorrect configuration - Using AllowAllAuthenticationProvider as default");
        return new AllowAllAuthenticationProvider();
    }

    @Provides
    @Singleton
    public AuthenticationHandler authenticationHandler(AuthenticationProvider authProvider)
    {
        return new MutualTlsAuthenticationHandler(authProvider);
    }

    @Provides
    @Singleton
    public SystemAuthSchema systemAuthSchema()
    {
        return new SystemAuthSchema();
    }

    @Provides
    @Singleton
    public SidecarConfiguration sidecarConfiguration() throws IOException
    {
        if (confPath == null)
        {
            throw new NullPointerException("the YAML configuration path for Sidecar has not been defined.");
        }
        return SidecarConfigurationImpl.readYamlConfiguration(confPath);
    }

    @Provides
    @Singleton
    public ServiceConfiguration serviceConfiguration(SidecarConfiguration sidecarConfiguration)
    {
        return sidecarConfiguration.serviceConfiguration();
    }

    @Provides
    @Singleton
    public CassandraInputValidationConfiguration validationConfiguration(SidecarConfiguration configuration)
    {
        return configuration.cassandraInputValidationConfiguration();
    }

    @Provides
    @Singleton
    public CQLSessionProvider cqlSessionProvider(Vertx vertx, SidecarConfiguration sidecarConfiguration,
                                                 DriverUtils driverUtils)
    {
        CQLSessionProviderImpl cqlSessionProvider = new CQLSessionProviderImpl(sidecarConfiguration,
                                                                               new NettyOptions(),
                                                                               driverUtils);
        vertx.eventBus().localConsumer(ON_SERVER_STOP.address(), message -> cqlSessionProvider.close());
        return cqlSessionProvider;
    }

    @Provides
    @Singleton
    public DriverUtils driverUtils()
    {
        return new DriverUtils();
    }

    @Provides
    @Singleton
    public InstancesConfig instancesConfig(Vertx vertx,
                                           SidecarConfiguration configuration,
                                           CassandraVersionProvider cassandraVersionProvider,
                                           SidecarVersionProvider sidecarVersionProvider,
                                           DnsResolver dnsResolver,
                                           CQLSessionProvider cqlSessionProvider,
                                           DriverUtils driverUtils,
                                           MetricRegistryFactory registryProvider)
    {
        List<InstanceMetadata> instanceMetadataList =
        configuration.cassandraInstances()
                     .stream()
                     .map(cassandraInstance -> {
                         JmxConfiguration jmxConfiguration = configuration.serviceConfiguration().jmxConfiguration();
                         return buildInstanceMetadata(vertx,
                                                      cassandraInstance,
                                                      cassandraVersionProvider,
                                                      sidecarVersionProvider.sidecarVersion(),
                                                      jmxConfiguration,
                                                      cqlSessionProvider,
                                                      driverUtils,
                                                      registryProvider);
                     })
                     .collect(Collectors.toList());

        return new InstancesConfigImpl(instanceMetadataList, dnsResolver);
    }

    @Provides
    @Singleton
    public CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver, DriverUtils driverUtils)
    {
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, driverUtils))
               .add(new Cassandra41Factory(dnsResolver, driverUtils))
               .build();
    }

    @Provides
    @Singleton
    @Named("StreamRequestRateLimiter")
    public SidecarRateLimiter streamRequestRateLimiter(ServiceConfiguration config)
    {
        long permitsPerSecond = config.throttleConfiguration().rateLimitStreamRequestsPerSecond();
        LOGGER.info("Configuring streamRequestRateLimiter. rateLimitStreamRequestsPerSecond={}",
                    permitsPerSecond);
        return SidecarRateLimiter.create(permitsPerSecond);
    }

    @Provides
    @Singleton
    @Named("IngressFileRateLimiter")
    public SidecarRateLimiter ingressFileRateLimiter(ServiceConfiguration config)
    {
        long bytesPerSecond = config.trafficShapingConfiguration()
                                    .inboundGlobalFileBandwidthBytesPerSecond();
        LOGGER.info("Configuring ingressFileRateLimiter. inboundGlobalFileBandwidth={}/s " +
                    "rawInboundGlobalFileBandwidth={} B/s", bytesToHumanReadableBinaryPrefix(bytesPerSecond),
                    bytesPerSecond);
        return SidecarRateLimiter.create(bytesPerSecond);
    }

    @Provides
    @Singleton
    public LoggerHandler loggerHandler()
    {
        return SidecarLoggerHandler.create(LoggerHandler.create());
    }

    @Provides
    @Singleton
    public TimeProvider timeProvider()
    {
        return TimeProvider.DEFAULT_TIME_PROVIDER;
    }

    @Provides
    @Singleton
    public ErrorHandler errorHandler()
    {
        return new JsonErrorHandler();
    }

    @Provides
    @Singleton
    public DnsResolver dnsResolver()
    {
        return DnsResolver.DEFAULT;
    }

    @Provides
    @Singleton
    public SidecarVersionProvider sidecarVersionProvider()
    {
        return new SidecarVersionProvider("/sidecar.version");
    }

    @Provides
    @Singleton
    public RestoreJobsSchema restoreJobsSchema(SidecarConfiguration configuration)
    {
        return new RestoreJobsSchema(configuration.serviceConfiguration()
                                                  .schemaKeyspaceConfiguration(),
                                     configuration.restoreJobConfiguration()
                                                  .restoreJobTablesTtlSeconds());
    }

    @Provides
    @Singleton
    public RestoreSlicesSchema restoreSlicesSchema(SidecarConfiguration configuration)
    {
        return new RestoreSlicesSchema(configuration.serviceConfiguration()
                                                    .schemaKeyspaceConfiguration(),
                                       configuration.restoreJobConfiguration()
                                                    .restoreJobTablesTtlSeconds());
    }

    @Provides
    @Singleton
    public RestoreRangesSchema restoreJobProgressSchema(SidecarConfiguration configuration)
    {
        return new RestoreRangesSchema(configuration.serviceConfiguration()
                                                    .schemaKeyspaceConfiguration(),
                                       configuration.restoreJobConfiguration()
                                                    .restoreJobTablesTtlSeconds());
    }

    @Provides
    @Singleton
    public SidecarSchema sidecarSchema(Vertx vertx,
                                       ExecutorPools executorPools,
                                       SidecarConfiguration configuration,
                                       CQLSessionProvider cqlSessionProvider,
                                       RestoreJobsSchema restoreJobsSchema,
                                       RestoreSlicesSchema restoreSlicesSchema,
                                       RestoreRangesSchema restoreRangesSchema,
                                       SystemAuthSchema systemAuthSchema,
                                       SidecarMetrics metrics)
    {
        SidecarInternalKeyspace sidecarInternalKeyspace = new SidecarInternalKeyspace(configuration);
        // register table schema when enabled
        sidecarInternalKeyspace.registerTableSchema(restoreJobsSchema);
        sidecarInternalKeyspace.registerTableSchema(restoreSlicesSchema);
        sidecarInternalKeyspace.registerTableSchema(restoreRangesSchema);
        sidecarInternalKeyspace.registerTableSchema(systemAuthSchema);
        SchemaMetrics schemaMetrics = metrics.server().schema();
        return new SidecarSchema(vertx, executorPools, configuration,
                                 sidecarInternalKeyspace, cqlSessionProvider, schemaMetrics);
    }

    @Provides
    @Singleton
    public SidecarMetrics metrics(MetricRegistryFactory registryFactory, InstanceMetadataFetcher metadataFetcher)
    {
        return new SidecarMetricsImpl(registryFactory, metadataFetcher);
    }

    /**
     * The provided hasher is used in {@link org.apache.cassandra.sidecar.restore.RestoreJobUtil}
     */
    @Provides
    @Singleton
    @Named("xxhash32")
    public DigestAlgorithmProvider xxHash32Provider()
    {
        return new XXHash32Provider();
    }

    @Provides
    @Singleton
    @Named("md5")
    public DigestAlgorithmProvider md5Provider()
    {
        return new JdkMd5DigestProvider();
    }

    @Provides
    @Singleton
    public LocalTokenRangesProvider localTokenRangesProvider(InstancesConfig instancesConfig, DnsResolver dnsResolver)
    {
        return new CachedLocalTokenRanges(instancesConfig, dnsResolver);
    }
}
