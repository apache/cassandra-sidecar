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

import java.io.File;

import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

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
import org.apache.cassandra.sidecar.routes.InfoService;

/**
 * Provides main binding for more complex Guice dependencies
 */
public class MainModule extends AbstractModule
{
    @Provides
    @Singleton
    public Vertx getVertx()
    {
        return Vertx.vertx(new VertxOptions().setMetricsOptions(
                new DropwizardMetricsOptions()
                        .setEnabled(true)
                        .setJmxEnabled(true)
                        .setJmxDomain("cassandra-sidecar-metrics")
        ));
    }

    @Provides
    @Singleton
    public HttpServer vertxServer(Vertx vertx, Configuration conf, Router router)
    {
        HttpServerOptions options = new HttpServerOptions().setLogActivity(true);

        if (conf.isSslEnabled())
        {
            options.setKeyStoreOptions(new JksOptions()
                                       .setPath(conf.getKeyStorePath())
                                       .setPassword(conf.getKeyStorePassword()))
                   .setSsl(conf.isSslEnabled());

            if (conf.getTrustStorePath() != null && conf.getTrustStorePassword() != null)
            {
                options.setTrustStoreOptions(new JksOptions()
                                             .setPath(conf.getTrustStorePath())
                                             .setPassword(conf.getTrustStorePassword()));
            }
        }

        HttpServer server = vertx.createHttpServer(options);
        server.requestHandler(router);
        return server;
    }

    @Provides
    @Singleton
    public Router vertxRouter(Vertx vertx, HealthService healthService, InfoService infoService)
    {
        Router router = Router.router(vertx);
        router.route().handler(LoggerHandler.create());

        // include docs generated into src/main/resources/docs
        StaticHandler swagger = StaticHandler.create()
                                             .setWebRoot("docs")
                                             .setCachingEnabled(false);
        router.route().path("/docs/*").handler(swagger);

        // API paths
        router.route().path("/api/v1/__health").handler(healthService::handleHealth);

        router.route().path("/api/v1/compactions").handler(infoService::handleCompactions);
        router.route().path("/api/v1/clients").handler(infoService::handleClients);
        router.route().path("/api/v1/settings").handler(infoService::handleSettings);
        router.route().path("/api/v1/threadpools").handler(infoService::handleThreadPools);

        return router;
    }

    @Provides
    @Singleton
    public Configuration configuration() throws ConfigurationException
    {
        Configurations confs = new Configurations();
        File propFile = new File("sidecar.yaml");
        YAMLConfiguration yamlConf = confs.fileBased(YAMLConfiguration.class, propFile);

        //CHECKSTYLE:OFF
        return new Configuration.Builder()
                           .withCassandraHost(yamlConf.get(String.class, "cassandra.host"))
                           .withCassandraPort(yamlConf.get(Integer.class, "cassandra.port"))
                           .withCassandraUsername(yamlConf.get(String.class, "cassandra.username", null))
                           .withCassandraPassword(yamlConf.get(String.class, "cassandra.password", null))
                           .withCassandraSslEnabled(yamlConf.get(Boolean.class, "cassandra.ssl.enabled", false))
                           .withCassandraTrustStorePath(yamlConf.get(String.class, "cassandra.ssl.truststore.path", null))
                           .withCassandraTrustStorePassword(yamlConf.get(String.class, "cassandra.ssl.truststore.password", null))
                           .withHost(yamlConf.get(String.class, "sidecar.host"))
                           .withPort(yamlConf.get(Integer.class, "sidecar.port"))
                           .withHealthCheckFrequencyMillis(yamlConf.get(Integer.class, "healthcheck.poll_freq_millis"))
                           .withKeyStorePath(yamlConf.get(String.class, "sidecar.ssl.keystore.path", null))
                           .withKeyStorePassword(yamlConf.get(String.class, "sidecar.ssl.keystore.password", null))
                           .withTrustStorePath(yamlConf.get(String.class, "sidecar.ssl.truststore.path", null))
                           .withTrustStorePassword(yamlConf.get(String.class, "sidecar.ssl.truststore.password", null))
                           .withSslEnabled(yamlConf.get(Boolean.class, "sidecar.ssl.enabled", false))
                           .build();
        //CHECKSTYLE:ON
    }
}
