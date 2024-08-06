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
import io.vertx.core.net.TrafficShapingOptions;
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
     * Updates the {@link TrafficShapingOptions} internally for the deployed server.
     *
     * @param options the updated traffic shaping options
     */
    void updateTrafficShapingOptions(TrafficShapingOptions options)
    {
        List<HttpServer> deployedServers = this.deployedServers;
        if (deployedServers == null || deployedServers.isEmpty())
        {
            throw new IllegalStateException("No servers are running");
        }
        deployedServers.forEach(server -> {
            try
            {
                server.updateTrafficShapingOptions(options);
            }
            catch (IllegalStateException ex)
            {
                // Sidecar always configures the traffic shaping option; such IllegalStateException is thrown due
                // to a vert.x bug.
                // See following comment for details
                if (ex.getMessage() != null
                    && ex.getMessage().contains("Unable to update traffic shaping options because the server was not configured to use traffic shaping during startup"))
                {
                    // TODO: we need to rollback this change once vert.x fixes this problem
                    // Swallowing the exception here is okay for now until we get a proper fix in vert.x.
                    // The way vert.x works is by creating a main io.vertx.core.net.impl.TCPServerBase
                    // object per host/port combination. Child handlers will inherit the main's
                    // io.netty.handler.traffic.GlobalTrafficShapingHandler since this configuration is a
                    // global configuration for a given host/port.
                    // As long as we are able to set the traffic shaping changes to the main server
                    // these changes will take effect on the child servers as well. A child server
                    // is created when there are multiple verticles configured.
                    LOGGER.warn("Unable to update traffic shaping options for server={}", server, ex);
                }
                else
                {
                    throw ex;
                }
            }
        });
    }

    /**
     * @return the actual port of the first deployed server
     * @throws IllegalStateException if no servers are deployed
     */
    @VisibleForTesting
    int actualPort()
    {
        if (deployedServers != null && !deployedServers.isEmpty())
            return deployedServers.get(0).actualPort();
        throw new IllegalStateException("No deployed server. Maybe server failed to deploy due to port conflict");
    }
}
