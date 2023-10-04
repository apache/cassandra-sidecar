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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.SSLOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.Router;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * The verticle implementation for a Sidecar server
 */
public class ServerVerticle extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerVerticle.class);

    protected final SidecarConfiguration sidecarConfiguration;
    protected final Router router;
    private final HttpServerOptions options;
    private List<HttpServer> deployedServers;

    /**
     * Constructs a new instance of the {@link ServerVerticle} with the provided parameters.
     *
     * @param sidecarConfiguration the configuration for running Sidecar
     * @param router               the configured router for this Service
     * @param options              the {@link HttpServerOptions} to create the HTTP server
     */
    public ServerVerticle(SidecarConfiguration sidecarConfiguration,
                          Router router,
                          HttpServerOptions options)
    {
        this.sidecarConfiguration = sidecarConfiguration;
        this.router = router;
        this.options = options;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(Promise<Void> startPromise)
    {
        ServiceConfiguration serviceConf = sidecarConfiguration.serviceConfiguration();

        List<SocketAddress> listenSocketAddresses = serviceConf.listenSocketAddresses();
        LOGGER.info("Deploying Cassandra Sidecar server verticle on socket addresses={}", listenSocketAddresses);
        List<Future<HttpServer>> futures = listenSocketAddresses.stream()
                                                                .map(socketAddress -> vertx.createHttpServer(options)
                                                                                           .requestHandler(router)
                                                                                           .listen(socketAddress))
                                                                .collect(Collectors.toList());
        Future.all(futures)
              .onSuccess((CompositeFuture startedServerFuture) -> {
                  deployedServers = new ArrayList<>(startedServerFuture.list());
                  LOGGER.info("Successfully deployed Cassandra Sidecar server verticle on socket addresses={}",
                              listenSocketAddresses);
                  startPromise.complete(); // notify that server started successfully
              })
              .onFailure(cause -> {
                  LOGGER.error("Failed to deploy Cassandra Sidecar verticle failed on socket addresses={}",
                               listenSocketAddresses, cause);
                  startPromise.fail(cause); // propagate failure to deploying class
              });
    }

    /**
     * Updates the {@link SSLOptions} internally for the deployed server
     *
     * @param options the updated SSL options
     * @return a future signaling the update success
     */
    Future<CompositeFuture> updateSSLOptions(SSLOptions options)
    {
        List<HttpServer> deployedServers = this.deployedServers;
        if (deployedServers == null || deployedServers.isEmpty())
        {
            return Future.failedFuture("No servers are running");
        }

        return Future.all(deployedServers.stream()
                                         .map(server -> server.updateSSLOptions(options))
                                         .collect(Collectors.toList()));
    }

    /**
     * @return the actual port of the first deployed server, or -1 if no servers are deployed
     */
    @VisibleForTesting
    int actualPort()
    {
        if (deployedServers != null && !deployedServers.isEmpty())
            return deployedServers.get(0).actualPort();
        return -1;
    }
}
