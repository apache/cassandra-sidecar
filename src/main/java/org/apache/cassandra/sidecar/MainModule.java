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

import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
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
import org.apache.cassandra.sidecar.routes.HealthService;
import org.apache.cassandra.sidecar.routes.SwaggerOpenApiResource;
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
    private VertxRequestHandler configureServices(Vertx vertx, HealthService healthService)
    {
        VertxResteasyDeployment deployment = new VertxResteasyDeployment();
        deployment.start();
        VertxRegistry r = deployment.getRegistry();

        r.addPerInstanceResource(SwaggerOpenApiResource.class);
        r.addSingletonResource(healthService);

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
    public Configuration configuration() throws ConfigurationException, IOException
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
                    .setCassandraHost(yamlConf.get(String.class, "cassandra.host"))
                    .setCassandraPort(yamlConf.get(Integer.class, "cassandra.port"))
                    .setHost(yamlConf.get(String.class, "sidecar.host"))
                    .setPort(yamlConf.get(Integer.class, "sidecar.port"))
                    .setHealthCheckFrequency(yamlConf.get(Integer.class, "healthcheck.poll_freq_millis"))
                    .setKeyStorePath(yamlConf.get(String.class, "sidecar.ssl.keystore.path", null))
                    .setKeyStorePassword(yamlConf.get(String.class, "sidecar.ssl.keystore.password", null))
                    .setTrustStorePath(yamlConf.get(String.class, "sidecar.ssl.truststore.path", null))
                    .setTrustStorePassword(yamlConf.get(String.class, "sidecar.ssl.truststore.password", null))
                    .setSslEnabled(yamlConf.get(Boolean.class, "sidecar.ssl.enabled", false))
                    .build();
        }
        catch (MalformedURLException e)
        {
            throw new ConfigurationException("Failed reading from sidebar.config path: " + confPath, e);
        }
    }
}
