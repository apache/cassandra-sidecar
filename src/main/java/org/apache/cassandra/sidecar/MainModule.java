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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.cassandra.sidecar.cassandra40.Cassandra40Factory;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.routes.CassandraHealthService;
import org.apache.cassandra.sidecar.routes.HealthService;
import org.apache.cassandra.sidecar.routes.StreamSSTableComponent;
import org.apache.cassandra.sidecar.routes.SwaggerOpenApiResource;
import org.apache.cassandra.sidecar.utils.YAMLKeyConstants;
import org.jboss.resteasy.plugins.server.vertx.VertxRegistry;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;

/**
 * Provides main binding for more complex Guice dependencies
 */
public class MainModule extends AbstractModule
{
    private static final Logger logger = LoggerFactory.getLogger(MainModule.class);

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
                                                  StreamSSTableComponent ssTableComponent,
                                                  CassandraHealthService cassandraHealthService)
    {
        VertxResteasyDeployment deployment = new VertxResteasyDeployment();
        deployment.start();
        VertxRegistry r = deployment.getRegistry();

        r.addPerInstanceResource(SwaggerOpenApiResource.class);
        r.addSingletonResource(healthService);
        r.addSingletonResource(ssTableComponent);
        r.addSingletonResource(cassandraHealthService);

        return new VertxRequestHandler(vertx, deployment);
    }

    @Provides
    @Singleton
    public Router vertxRouter(Vertx vertx)
    {
        Router router = Router.router(vertx);
        router.route().handler(LoggerHandler.create());

        // Static web assets for Swagger
        StaticHandler swaggerStatic = StaticHandler.create("META-INF/resources/webjars/swagger-ui");
        router.route().path("/static/swagger-ui/*").handler(swaggerStatic);

        // Docs index.html page
        StaticHandler docs = StaticHandler.create("docs");
        router.route().path("/docs/*").handler(docs);
        return router;
    }

    @Provides
    @Singleton
    public Configuration configuration(InstancesConfig instancesConfig) throws ConfigurationException, IOException
    {
        final String confPath = System.getProperty("sidecar.config", "file://./conf/config.yaml");
        logger.info("Reading configuration from {}", confPath);
        try
        {
            URL url = new URL(confPath);

            YAMLConfiguration yamlConf = new YAMLConfiguration();
            InputStream stream = url.openStream();
            yamlConf.read(stream);

            return new Configuration.Builder()
                    .setInstancesConfig(instancesConfig)
                    .setHost(yamlConf.get(String.class, YAMLKeyConstants.HOST))
                    .setPort(yamlConf.get(Integer.class, YAMLKeyConstants.PORT))
                    .setHealthCheckFrequency(yamlConf.get(Integer.class, YAMLKeyConstants.HEALTH_CHECK_FREQ))
                    .setKeyStorePath(yamlConf.get(String.class, YAMLKeyConstants.KEYSTORE_PATH, null))
                    .setKeyStorePassword(yamlConf.get(String.class, YAMLKeyConstants.KEYSTORE_PASSWORD, null))
                    .setTrustStorePath(yamlConf.get(String.class, YAMLKeyConstants.TRUSTSTORE_PATH, null))
                    .setTrustStorePassword(yamlConf.get(String.class, YAMLKeyConstants.TRUSTSTORE_PASSWORD, null))
                    .setSslEnabled(yamlConf.get(Boolean.class, YAMLKeyConstants.SSL_ENABLED, false))
                    .setRateLimitStreamRequestsPerSecond(yamlConf.getLong(YAMLKeyConstants.STREAM_REQUESTS_PER_SEC))
                    .setThrottleTimeoutInSeconds(yamlConf.getLong(YAMLKeyConstants.THROTTLE_TIMEOUT_SEC))
                    .setThrottleDelayInSeconds(yamlConf.getLong(YAMLKeyConstants.THROTTLE_DELAY_SEC))
                    .build();
        }
        catch (MalformedURLException e)
        {
            throw new ConfigurationException("Failed reading from sidebar.config path: " + confPath, e);
        }
    }

    @Provides
    @Singleton
    public InstancesConfig getInstancesConfig(CassandraVersionProvider versionProvider)
        throws ConfigurationException, IOException
    {
        final String confPath = System.getProperty("sidecar.config", "file://./conf/config.yaml");
        URL url = new URL(confPath);

        try
        {
            YAMLConfiguration yamlConf = new YAMLConfiguration();
            InputStream stream = url.openStream();
            yamlConf.read(stream);
            return readInstancesConfig(yamlConf, versionProvider);
        }
        catch (MalformedURLException e)
        {
            throw new ConfigurationException(String.format("Unable to parse cluster information from '%s'", url));
        }
    }

    @VisibleForTesting
    public InstancesConfigImpl readInstancesConfig(YAMLConfiguration yamlConf, CassandraVersionProvider versionProvider)
    {
        final int healthCheckFrequencyMillis = yamlConf.get(Integer.class, YAMLKeyConstants.HEALTH_CHECK_FREQ);

        /* Since we are supporting handling multiple instances in Sidecar optionally, we prefer reading single instance
         * data over reading multiple instances section
         */
        org.apache.commons.configuration2.Configuration singleInstanceConf = yamlConf.subset(YAMLKeyConstants.CASSANDRA_INSTANCE);
        if (singleInstanceConf != null && !singleInstanceConf.isEmpty())
        {
            String host = singleInstanceConf.get(String.class, YAMLKeyConstants.CASSANDRA_INSTANCE_HOST);
            int port = singleInstanceConf.get(Integer.class, YAMLKeyConstants.CASSANDRA_INSTANCE_PORT);
            String dataDirs = singleInstanceConf.get(String.class, YAMLKeyConstants.CASSANDRA_INSTANCE_DATA_DIRS);
            CQLSession session = new CQLSession(host, port, healthCheckFrequencyMillis);
            return new InstancesConfigImpl(Collections.singletonList(new InstanceMetadataImpl(1, host, port,
                Collections.unmodifiableList(Arrays.asList(dataDirs.split(","))), session,
                versionProvider, healthCheckFrequencyMillis)));
        }

        List<HierarchicalConfiguration<ImmutableNode>> instances = yamlConf.configurationsAt(
        YAMLKeyConstants.CASSANDRA_INSTANCES);
        final List<InstanceMetadata> instanceMetas = new LinkedList<>();
        for (HierarchicalConfiguration<ImmutableNode> instance : instances)
        {
            int id = instance.get(Integer.class, YAMLKeyConstants.CASSANDRA_INSTANCE_ID);
            String host = instance.get(String.class, YAMLKeyConstants.CASSANDRA_INSTANCE_HOST);
            int port = instance.get(Integer.class, YAMLKeyConstants.CASSANDRA_INSTANCE_PORT);
            String dataDirs = instance.get(String.class, YAMLKeyConstants.CASSANDRA_INSTANCE_DATA_DIRS);

            CQLSession session = new CQLSession(host, port, healthCheckFrequencyMillis);
            instanceMetas.add(new InstanceMetadataImpl(id, host, port,
                Collections.unmodifiableList(Arrays.asList(dataDirs.split(","))), session, versionProvider,
                healthCheckFrequencyMillis));
        }
        return new InstancesConfigImpl(instanceMetas);
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
    public SidecarRateLimiter rateLimiter(Configuration config)
    {
        return SidecarRateLimiter.create(config.getRateLimitStreamRequestsPerSecond());
    }
}
