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

package org.apache.cassandra.sidecar;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SidecarRateLimiter;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import org.apache.cassandra.sidecar.cassandra40.Cassandra40Factory;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;
import org.apache.cassandra.sidecar.logging.SidecarLoggerHandler;
import org.apache.cassandra.sidecar.routes.CassandraHealthService;
import org.apache.cassandra.sidecar.routes.FileStreamHandler;
import org.apache.cassandra.sidecar.routes.GossipInfoHandler;
import org.apache.cassandra.sidecar.routes.HealthService;
import org.apache.cassandra.sidecar.routes.JsonErrorHandler;
import org.apache.cassandra.sidecar.routes.RingHandler;
import org.apache.cassandra.sidecar.routes.SchemaHandler;
import org.apache.cassandra.sidecar.routes.SnapshotsHandler;
import org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.routes.SwaggerOpenApiResource;
import org.apache.cassandra.sidecar.routes.TimeSkewHandler;
import org.apache.cassandra.sidecar.routes.cassandra.NodeSettingsHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableCleanupHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableImportHandler;
import org.apache.cassandra.sidecar.routes.sstableuploads.SSTableUploadHandler;
import org.apache.cassandra.sidecar.utils.ChecksumVerifier;
import org.apache.cassandra.sidecar.utils.MD5ChecksumVerifier;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.jboss.resteasy.plugins.server.vertx.VertxRegistry;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;

/**
 * Provides main binding for more complex Guice dependencies
 */
public class MainModule extends AbstractModule
{
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
    public HttpServer vertxServer(Vertx vertx, Configuration conf, Router router, VertxRequestHandler restHandler)
    {
        HttpServerOptions options = new HttpServerOptions().setLogActivity(true);
        options.setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
               .setIdleTimeout(conf.getRequestIdleTimeoutMillis());

        if (conf.isSslEnabled())
        {
            options.setKeyStoreOptions(new JksOptions()
                                       .setPath(conf.getKeyStorePath())
                                       .setPassword(conf.getKeystorePassword()))
                   .setSsl(conf.isSslEnabled());

            if (conf.getTrustStorePath() != null && conf.getTruststorePassword() != null)
            {
                options.setTrustStoreOptions(new JksOptions()
                                             .setPath(conf.getTrustStorePath())
                                             .setPassword(conf.getTruststorePassword()));
            }
        }

        router.route().pathRegex(".*").handler(rc -> restHandler.handle(rc.request()));

        return vertx.createHttpServer(options)
                    .requestHandler(router);
    }

    @Provides
    @Singleton
    private VertxRequestHandler configureServices(Vertx vertx,
                                                  HealthService healthService,
                                                  CassandraHealthService cassandraHealthService)
    {
        VertxResteasyDeployment deployment = new VertxResteasyDeployment();
        deployment.start();
        VertxRegistry registry = deployment.getRegistry();

        registry.addPerInstanceResource(SwaggerOpenApiResource.class);
        registry.addSingletonResource(healthService);
        registry.addSingletonResource(cassandraHealthService);

        return new VertxRequestHandler(vertx, deployment);
    }

    @Provides
    @Singleton
    public Router vertxRouter(Vertx vertx,
                              Configuration conf,
                              StreamSSTableComponentHandler streamSSTableComponentHandler,
                              FileStreamHandler fileStreamHandler,
                              SnapshotsHandler snapshotsHandler,
                              SchemaHandler schemaHandler,
                              RingHandler ringHandler,
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
              .handler(TimeoutHandler.create(conf.getRequestTimeoutMillis(),
                                             HttpResponseStatus.REQUEST_TIMEOUT.code()));

        router.route()
              .path(ApiEndpointsV1.API + "/*")
              .failureHandler(errorHandler);

        // Static web assets for Swagger
        StaticHandler swaggerStatic = StaticHandler.create("META-INF/resources/webjars/swagger-ui");
        router.route()
              .path("/static/swagger-ui/*")
              .handler(swaggerStatic);

        // Docs index.html page
        StaticHandler docs = StaticHandler.create("docs");
        router.route()
              .path("/docs/*")
              .handler(docs);

        // Add custom routers
        //noinspection deprecation
        router.get(ApiEndpointsV1.DEPRECATED_COMPONENTS_ROUTE)
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        router.get(ApiEndpointsV1.COMPONENTS_ROUTE)
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
    public Configuration configuration(CassandraVersionProvider cassandraVersionProvider)
    throws IOException
    {
        final String confPath = System.getProperty("sidecar.config", "file://./conf/config.yaml");
        return YAMLSidecarConfiguration.of(confPath, cassandraVersionProvider);
    }

    @Provides
    @Singleton
    public InstancesConfig instancesConfig(Configuration configuration)
    {
        return configuration.getInstancesConfig();
    }

    @Provides
    @Singleton
    public ValidationConfiguration validationConfiguration(Configuration configuration)
    {
        return configuration.getValidationConfiguration();
    }

    @Provides
    @Singleton
    public CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver)
    {
        CassandraVersionProvider.Builder builder = new CassandraVersionProvider.Builder();
        builder.add(new Cassandra40Factory(dnsResolver));
        return builder.build();
    }

    @Provides
    @Singleton
    public SidecarRateLimiter streamRequestRateLimiter(Configuration config)
    {
        return SidecarRateLimiter.create(config.getRateLimitStreamRequestsPerSecond());
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
    public ErrorHandler errorHandler(Vertx vertx)
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
}
