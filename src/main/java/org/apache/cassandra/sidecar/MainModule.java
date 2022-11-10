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

import com.google.common.util.concurrent.SidecarRateLimiter;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.cassandra.sidecar.cassandra40.Cassandra40Factory;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;
import org.apache.cassandra.sidecar.routes.CassandraHealthService;
import org.apache.cassandra.sidecar.routes.CassandraSettingsService;
import org.apache.cassandra.sidecar.routes.FileStreamHandler;
import org.apache.cassandra.sidecar.routes.HealthService;
import org.apache.cassandra.sidecar.routes.ListSnapshotFilesHandler;
import org.apache.cassandra.sidecar.routes.SchemaHandler;
import org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.routes.SwaggerOpenApiResource;
import org.jboss.resteasy.plugins.server.vertx.VertxRegistry;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;

/**
 * Provides main binding for more complex Guice dependencies
 */
public class MainModule extends AbstractModule
{
    private static final String API_V1_VERSION = "/api/v1";

    @Override
    protected void configure()
    {
        requestStaticInjection(QualifiedTableName.class);
    }

    @Provides
    @Singleton
    public Vertx getVertx()
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
                                                  CassandraHealthService cassandraHealthService,
                                                  CassandraSettingsService cassandraSettingsService)
    {
        VertxResteasyDeployment deployment = new VertxResteasyDeployment();
        deployment.start();
        VertxRegistry r = deployment.getRegistry();

        r.addPerInstanceResource(SwaggerOpenApiResource.class);
        r.addSingletonResource(healthService);
        r.addSingletonResource(cassandraHealthService);
        r.addSingletonResource(cassandraSettingsService);

        return new VertxRequestHandler(vertx, deployment);
    }

    @Provides
    @Singleton
    public Router vertxRouter(Vertx vertx,
                              StreamSSTableComponentHandler streamSSTableComponentHandler,
                              FileStreamHandler fileStreamHandler,
                              ListSnapshotFilesHandler listSnapshotFilesHandler,
                              SchemaHandler schemaHandler,
                              LoggerHandler loggerHandler,
                              ErrorHandler errorHandler)
    {
        Router router = Router.router(vertx);
        router.route()
              .failureHandler(errorHandler)
              .handler(loggerHandler);

        // Static web assets for Swagger
        StaticHandler swaggerStatic = StaticHandler.create("META-INF/resources/webjars/swagger-ui");
        router.route().path("/static/swagger-ui/*").handler(swaggerStatic);

        // Docs index.html page
        StaticHandler docs = StaticHandler.create("docs");
        router.route().path("/docs/*").handler(docs);

        // add custom routers
        final String componentRoute = "/keyspace/:keyspace/table/:table/snapshots/:snapshot/component/:component";
        router.get(API_V1_VERSION + componentRoute)
              .handler(streamSSTableComponentHandler)
              .handler(fileStreamHandler);

        final String listSnapshotFilesRoute = "/keyspace/:keyspace/table/:table/snapshots/:snapshot";
        router.get(API_V1_VERSION + listSnapshotFilesRoute)
              .handler(listSnapshotFilesHandler);

        final String allKeyspacesSchemasRoute = "/schema/keyspaces";
        router.get(API_V1_VERSION + allKeyspacesSchemasRoute)
              .handler(schemaHandler);

        final String keyspaceSchemaRoute = "/schema/keyspaces/:keyspace";
        router.get(API_V1_VERSION + keyspaceSchemaRoute)
              .handler(schemaHandler);

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
    public InstancesConfig getInstancesConfig(Configuration configuration)
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
    public CassandraVersionProvider cassandraVersionProvider()
    {
        CassandraVersionProvider.Builder builder = new CassandraVersionProvider.Builder();
        builder.add(new Cassandra40Factory());
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
        return LoggerHandler.create();
    }

    @Provides
    @Singleton
    public ErrorHandler errorHandler(Vertx vertx)
    {
        return ErrorHandler.create(vertx);
    }
}
