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
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SidecarRateLimiter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
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
import org.apache.cassandra.sidecar.cassandra40.Cassandra40Factory;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;
import org.apache.cassandra.sidecar.common.utils.YAMLValidationConfiguration;
import org.apache.cassandra.sidecar.routes.CassandraHealthService;
import org.apache.cassandra.sidecar.routes.FileStreamHandler;
import org.apache.cassandra.sidecar.routes.HealthService;
import org.apache.cassandra.sidecar.routes.KeyspacesHandler;
import org.apache.cassandra.sidecar.routes.ListSnapshotFilesHandler;
import org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.routes.SwaggerOpenApiResource;
import org.jboss.resteasy.plugins.server.vertx.VertxRegistry;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;

import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_ALLOWED_CHARS_FOR_COMPONENT_NAME;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_ALLOWED_CHARS_FOR_DIRECTORY;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_FORBIDDEN_KEYSPACES;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_INPUT_VALIDATION;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_INSTANCE;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_INSTANCES;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_INSTANCE_DATA_DIRS;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_INSTANCE_HOST;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_INSTANCE_ID;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.CASSANDRA_INSTANCE_PORT;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.HEALTH_CHECK_FREQ;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.HOST;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.KEYSTORE_PASSWORD;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.KEYSTORE_PATH;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.PORT;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.SSL_ENABLED;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.STREAM_REQUESTS_PER_SEC;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.THROTTLE_DELAY_SEC;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.THROTTLE_TIMEOUT_SEC;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.TRUSTSTORE_PASSWORD;
import static org.apache.cassandra.sidecar.utils.YAMLKeyConstants.TRUSTSTORE_PATH;

/**
 * Provides main binding for more complex Guice dependencies
 */
public class MainModule extends AbstractModule
{
    private static final Logger logger = LoggerFactory.getLogger(MainModule.class);
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
                                                  CassandraHealthService cassandraHealthService)
    {
        VertxResteasyDeployment deployment = new VertxResteasyDeployment();
        deployment.start();
        VertxRegistry r = deployment.getRegistry();

        r.addPerInstanceResource(SwaggerOpenApiResource.class);
        r.addSingletonResource(healthService);
        r.addSingletonResource(cassandraHealthService);

        return new VertxRequestHandler(vertx, deployment);
    }

    @Provides
    @Singleton
    public Router vertxRouter(Vertx vertx,
                              StreamSSTableComponentHandler streamSSTableComponentHandler,
                              FileStreamHandler fileStreamHandler,
                              ListSnapshotFilesHandler listSnapshotFilesHandler,
                              KeyspacesHandler keyspacesHandler,
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

        final String keyspacesRoute = "/keyspaces";
        router.get(API_V1_VERSION + keyspacesRoute)
              .handler(keyspacesHandler);

        final String keyspaceRoute = "/keyspace/:keyspace";
        router.route()
              .method(HttpMethod.HEAD)
              .method(HttpMethod.GET)
              .path(API_V1_VERSION + keyspaceRoute)
              .handler(keyspacesHandler);

        final String tableRoute = "/keyspace/:keyspace/table/:table";
        router.route()
              .method(HttpMethod.HEAD)
              .method(HttpMethod.GET)
              .path(API_V1_VERSION + tableRoute)
              .handler(keyspacesHandler);

        return router;
    }

    @Provides
    @Singleton
    public Configuration configuration(YAMLConfiguration yamlConf, InstancesConfig instancesConfig,
                                       ValidationConfiguration validationConfiguration)
    throws IOException
    {
        return new Configuration.Builder()
               .setInstancesConfig(instancesConfig)
               .setHost(yamlConf.get(String.class, HOST))
               .setPort(yamlConf.get(Integer.class, PORT))
               .setHealthCheckFrequency(yamlConf.get(Integer.class, HEALTH_CHECK_FREQ))
               .setKeyStorePath(yamlConf.get(String.class, KEYSTORE_PATH, null))
               .setKeyStorePassword(yamlConf.get(String.class, KEYSTORE_PASSWORD, null))
               .setTrustStorePath(yamlConf.get(String.class, TRUSTSTORE_PATH, null))
               .setTrustStorePassword(yamlConf.get(String.class, TRUSTSTORE_PASSWORD, null))
               .setSslEnabled(yamlConf.get(Boolean.class, SSL_ENABLED, false))
               .setRateLimitStreamRequestsPerSecond(yamlConf.getLong(STREAM_REQUESTS_PER_SEC))
               .setThrottleTimeoutInSeconds(yamlConf.getLong(THROTTLE_TIMEOUT_SEC))
               .setThrottleDelayInSeconds(yamlConf.getLong(THROTTLE_DELAY_SEC))
               .setValidationConfiguration(validationConfiguration)
               .build();
    }

    @Provides
    @Singleton
    public InstancesConfig getInstancesConfig(YAMLConfiguration yamlConf, CassandraVersionProvider versionProvider)
    {
        return readInstancesConfig(yamlConf, versionProvider);
    }

    @Provides
    @Singleton
    public ValidationConfiguration validationConfiguration(YAMLConfiguration yamlConf)
    {
        org.apache.commons.configuration2.Configuration validation = yamlConf.subset(CASSANDRA_INPUT_VALIDATION);
        Set<String> forbiddenKeyspaces = new HashSet<>(validation.getList(String.class,
                                                                          CASSANDRA_FORBIDDEN_KEYSPACES,
                                                                          Collections.emptyList()));
        UnaryOperator<String> readString = key -> validation.get(String.class, key);
        String allowedPatternForDirectory = readString.apply(CASSANDRA_ALLOWED_CHARS_FOR_DIRECTORY);
        String allowedPatternForComponentName = readString.apply(CASSANDRA_ALLOWED_CHARS_FOR_COMPONENT_NAME);
        String allowedPatternForRestrictedComponentName = readString
                                                          .apply(CASSANDRA_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME);

        return new YAMLValidationConfiguration(forbiddenKeyspaces,
                                               allowedPatternForDirectory,
                                               allowedPatternForComponentName,
                                               allowedPatternForRestrictedComponentName);
    }

    @VisibleForTesting
    public InstancesConfigImpl readInstancesConfig(YAMLConfiguration yamlConf, CassandraVersionProvider versionProvider)
    {
        final int healthCheckFrequencyMillis = yamlConf.get(Integer.class, HEALTH_CHECK_FREQ);

        /* Since we are supporting handling multiple instances in Sidecar optionally, we prefer reading single instance
         * data over reading multiple instances section
         */
        org.apache.commons.configuration2.Configuration singleInstanceConf = yamlConf.subset(CASSANDRA_INSTANCE);
        if (singleInstanceConf != null && !singleInstanceConf.isEmpty())
        {
            String host = singleInstanceConf.get(String.class, CASSANDRA_INSTANCE_HOST);
            int port = singleInstanceConf.get(Integer.class, CASSANDRA_INSTANCE_PORT);
            String dataDirs = singleInstanceConf.get(String.class, CASSANDRA_INSTANCE_DATA_DIRS);
            CQLSession session = new CQLSession(host, port, healthCheckFrequencyMillis);
            return new InstancesConfigImpl(Collections.singletonList(new InstanceMetadataImpl(1, host, port,
                Collections.unmodifiableList(Arrays.asList(dataDirs.split(","))), session,
                versionProvider, healthCheckFrequencyMillis)));
        }

        List<HierarchicalConfiguration<ImmutableNode>> instances = yamlConf.configurationsAt(CASSANDRA_INSTANCES);
        final List<InstanceMetadata> instanceMetas = new LinkedList<>();
        for (HierarchicalConfiguration<ImmutableNode> instance : instances)
        {
            int id = instance.get(Integer.class, CASSANDRA_INSTANCE_ID);
            String host = instance.get(String.class, CASSANDRA_INSTANCE_HOST);
            int port = instance.get(Integer.class, CASSANDRA_INSTANCE_PORT);
            String dataDirs = instance.get(String.class, CASSANDRA_INSTANCE_DATA_DIRS);

            CQLSession session = new CQLSession(host, port, healthCheckFrequencyMillis);
            instanceMetas.add(new InstanceMetadataImpl(id, host, port,
                Collections.unmodifiableList(Arrays.asList(dataDirs.split(","))), session, versionProvider,
                healthCheckFrequencyMillis));
        }
        return new InstancesConfigImpl(instanceMetas);
    }

    @Provides
    @Singleton
    public YAMLConfiguration yamlConfiguration() throws ConfigurationException
    {
        final String confPath = System.getProperty("sidecar.config", "file://./conf/config.yaml");
        logger.info("Reading configuration from {}", confPath);

        try
        {
            URL url = new URL(confPath);
            YAMLConfiguration yamlConf = new YAMLConfiguration();
            InputStream stream = url.openStream();
            yamlConf.read(stream);
            return yamlConf;
        }
        catch (IOException e)
        {
            throw new ConfigurationException(String.format("Unable to parse cluster information from '%s'", confPath));
        }
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
