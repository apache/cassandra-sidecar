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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.SidecarRateLimiter;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.logging.SidecarLoggerHandler;
import org.apache.cassandra.sidecar.routes.CassandraHealthHandler;
import org.apache.cassandra.sidecar.routes.FileStreamHandler;
import org.apache.cassandra.sidecar.routes.GossipInfoHandler;
import org.apache.cassandra.sidecar.routes.JsonErrorHandler;
import org.apache.cassandra.sidecar.routes.RingHandler;
import org.apache.cassandra.sidecar.routes.SchemaHandler;
import org.apache.cassandra.sidecar.routes.SnapshotsHandler;
import org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.routes.TimeSkewHandler;
import org.apache.cassandra.sidecar.routes.TokenRangeReplicaMapHandler;
import org.apache.cassandra.sidecar.routes.cassandra.NodeSettingsHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableCleanupHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableImportHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableUploadHandler;
import org.apache.cassandra.sidecar.stats.SidecarStats;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.ChecksumVerifier;
import org.apache.cassandra.sidecar.utils.MD5ChecksumVerifier;
import org.apache.cassandra.sidecar.utils.TimeProvider;

/**
 * Provides main binding for more complex Guice dependencies
 */
public class MainModule extends AbstractModule
{
    public static final Map<String, String> OK_STATUS = Collections.singletonMap("status", "OK");
    public static final Map<String, String> NOT_OK_STATUS = Collections.singletonMap("status", "NOT_OK");

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

    @Provides
    @Singleton
    public Vertx vertx()
    {
        return Vertx.vertx(new VertxOptions().setMetricsOptions(new DropwizardMetricsOptions()
                                                                .setEnabled(true)
                                                                .setJmxEnabled(true)
                                                                .setJmxDomain("cassandra-sidecar-metrics")));
    }

    @Provides
    @Singleton
    public Router vertxRouter(Vertx vertx,
                              ServiceConfiguration conf,
                              CassandraHealthHandler cassandraHealthHandler,
                              StreamSSTableComponentHandler streamSSTableComponentHandler,
                              FileStreamHandler fileStreamHandler,
                              SnapshotsHandler snapshotsHandler,
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
                              ErrorHandler errorHandler)
    {
        Router router = Router.router(vertx);
        router.route()
              .handler(loggerHandler)
              .handler(TimeoutHandler.create(conf.requestTimeoutMillis(),
                                             HttpResponseStatus.REQUEST_TIMEOUT.code()));

        router.route()
              .path(ApiEndpointsV1.API + "/*")
              .failureHandler(errorHandler);

        // Docs index.html page
        StaticHandler docs = StaticHandler.create("docs");
        router.route()
              .path("/docs/*")
              .handler(docs);

        // Add custom routers
        // Provides a simple REST endpoint to determine if Sidecar is available
        router.get(ApiEndpointsV1.HEALTH_ROUTE)
              .handler(context -> context.json(OK_STATUS));

        router.get(ApiEndpointsV1.CASSANDRA_HEALTH_ROUTE)
              .handler(cassandraHealthHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_COMPONENTS_ROUTE)
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        router.get(ApiEndpointsV1.COMPONENTS_ROUTE)
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        // Support for routes that want to stream SStable index components
        router.get(ApiEndpointsV1.COMPONENTS_WITH_SECONDARY_INDEX_ROUTE_SUPPORT)
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_SNAPSHOTS_ROUTE)
              .handler(snapshotsHandler);

        router.route()
              .method(HttpMethod.GET)
              .method(HttpMethod.PUT)
              .method(HttpMethod.DELETE)
              .path(ApiEndpointsV1.SNAPSHOTS_ROUTE)
              .handler(snapshotsHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_ALL_KEYSPACES_SCHEMA_ROUTE)
              .handler(schemaHandler);

        router.get(ApiEndpointsV1.ALL_KEYSPACES_SCHEMA_ROUTE)
              .handler(schemaHandler);

        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_KEYSPACE_SCHEMA_ROUTE)
              .handler(schemaHandler);

        router.get(ApiEndpointsV1.KEYSPACE_SCHEMA_ROUTE)
              .handler(schemaHandler);

        router.get(ApiEndpointsV1.RING_ROUTE)
              .handler(ringHandler);

        router.get(ApiEndpointsV1.RING_ROUTE_PER_KEYSPACE)
              .handler(ringHandler);

        router.put(ApiEndpointsV1.SSTABLE_UPLOAD_ROUTE)
              .handler(ssTableUploadHandler);

        router.get(ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE)
              .handler(tokenRangeHandler);

        router.put(ApiEndpointsV1.SSTABLE_IMPORT_ROUTE)
              .handler(ssTableImportHandler);

        router.delete(ApiEndpointsV1.SSTABLE_CLEANUP_ROUTE)
              .handler(ssTableCleanupHandler);

        router.get(ApiEndpointsV1.GOSSIP_INFO_ROUTE)
              .handler(gossipInfoHandler);

        router.get(ApiEndpointsV1.TIME_SKEW_ROUTE)
              .handler(timeSkewHandler);

        router.get(ApiEndpointsV1.NODE_SETTINGS_ROUTE)
              .handler(nodeSettingsHandler);

        return router;
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
    public InstancesConfig instancesConfig(Vertx vertx,
                                           SidecarConfiguration configuration,
                                           CassandraVersionProvider cassandraVersionProvider,
                                           SidecarVersionProvider sidecarVersionProvider,
                                           DnsResolver dnsResolver)
    {
        int healthCheckFrequencyMillis = configuration.healthCheckConfiguration().checkIntervalMillis();

        List<InstanceMetadata> instanceMetadataList =
        configuration.cassandraInstances()
                     .stream()
                     .map(cassandraInstance -> {
                         JmxConfiguration jmxConfiguration = configuration.serviceConfiguration().jmxConfiguration();
                         return buildInstanceMetadata(vertx,
                                                      cassandraInstance,
                                                      cassandraVersionProvider,
                                                      healthCheckFrequencyMillis,
                                                      sidecarVersionProvider.sidecarVersion(),
                                                      jmxConfiguration);
                     })
                     .collect(Collectors.toList());

        return new InstancesConfigImpl(instanceMetadataList, dnsResolver);
    }

    @Provides
    @Singleton
    public CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver,
                                                             SidecarVersionProvider sidecarVersionProvider)
    {
        CassandraVersionProvider.Builder builder = new CassandraVersionProvider.Builder();
        builder.add(new CassandraFactory(dnsResolver, sidecarVersionProvider.sidecarVersion()));
        return builder.build();
    }

    @Provides
    @Singleton
    public SidecarRateLimiter streamRequestRateLimiter(ServiceConfiguration config)
    {
        return SidecarRateLimiter.create(config.throttleConfiguration()
                                               .rateLimitStreamRequestsPerSecond());
    }

    @Provides
    @Singleton
    @Named("IngressFileRateLimiter")
    public SidecarRateLimiter ingressFileRateLimiter(ServiceConfiguration serviceConfiguration)
    {
        return SidecarRateLimiter.create(serviceConfiguration.trafficShapingConfiguration()
                                                             .inboundGlobalFileBandwidthBytesPerSecond());
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
    public ChecksumVerifier checksumVerifier(Vertx vertx)
    {
        return new MD5ChecksumVerifier(vertx.fileSystem());
    }

    @Provides
    @Singleton
    public SidecarVersionProvider sidecarVersionProvider()
    {
        return new SidecarVersionProvider("/sidecar.version");
    }

    @Provides
    @Singleton
    public SidecarStats sidecarStats()
    {
        return SidecarStats.INSTANCE;
    }

    /**
     * Builds the {@link InstanceMetadata} from the {@link InstanceConfiguration},
     * a provided {@code  versionProvider}, and {@code healthCheckFrequencyMillis}.
     *
     * @param vertx                      the vertx instance
     * @param cassandraInstance          the cassandra instance configuration
     * @param versionProvider            a Cassandra version provider
     * @param healthCheckFrequencyMillis the health check frequency configuration in milliseconds
     * @param sidecarVersion             the version of the Sidecar from the current binary
     * @param jmxConfiguration           the configuration for the JMX Client
     * @return the build instance metadata object
     */
    private static InstanceMetadata buildInstanceMetadata(Vertx vertx,
                                                          InstanceConfiguration cassandraInstance,
                                                          CassandraVersionProvider versionProvider,
                                                          int healthCheckFrequencyMillis,
                                                          String sidecarVersion,
                                                          JmxConfiguration jmxConfiguration)
    {
        String host = cassandraInstance.host();
        int port = cassandraInstance.port();

        CQLSessionProvider session = new CQLSessionProvider(host, port, healthCheckFrequencyMillis);
        JmxClient jmxClient = JmxClient.builder()
                                       .host(cassandraInstance.jmxHost())
                                       .port(cassandraInstance.jmxPort())
                                       .role(cassandraInstance.jmxRole())
                                       .password(cassandraInstance.jmxRolePassword())
                                       .enableSsl(cassandraInstance.jmxSslEnabled())
                                       .connectionMaxRetries(jmxConfiguration.maxRetries())
                                       .connectionRetryDelayMillis(jmxConfiguration.retryDelayMillis())
                                       .build();
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
                                                                         cassandraInstance.id(),
                                                                         versionProvider,
                                                                         session,
                                                                         jmxClient,
                                                                         sidecarVersion);
        return InstanceMetadataImpl.builder()
                                   .id(cassandraInstance.id())
                                   .host(host)
                                   .port(port)
                                   .dataDirs(cassandraInstance.dataDirs())
                                   .stagingDir(cassandraInstance.stagingDir())
                                   .delegate(delegate)
                                   .build();
    }
}
