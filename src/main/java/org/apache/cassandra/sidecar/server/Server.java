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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.SSLOptions;
import io.vertx.ext.web.Router;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.tasks.KeyStoreCheckPeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.utils.SslUtils;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * The Sidecar {@link Server} class that manages the start and stop lifecycle of the service
 */
@Singleton
public class Server
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    protected final Vertx vertx;
    protected final ExecutorPools executorPools;
    protected final SidecarConfiguration sidecarConfiguration;
    protected final InstancesConfig instancesConfig;
    protected final Router router;
    private final PeriodicTaskExecutor periodicTaskExecutor;
    private final HttpServerOptionsProvider optionsProvider;
    private final SidecarServerEvents serverEvents;
    protected final List<ServerVerticle> deployedServerVerticles = new CopyOnWriteArrayList<>();

    @Inject
    public Server(Vertx vertx,
                  SidecarConfiguration sidecarConfiguration,
                  Router router,
                  InstancesConfig instancesConfig,
                  ExecutorPools executorPools,
                  PeriodicTaskExecutor periodicTaskExecutor,
                  HttpServerOptionsProvider optionsProvider,
                  SidecarServerEvents serverEvents)
    {
        this.vertx = vertx;
        this.executorPools = executorPools;
        this.sidecarConfiguration = sidecarConfiguration;
        this.instancesConfig = instancesConfig;
        this.router = router;
        this.periodicTaskExecutor = periodicTaskExecutor;
        this.optionsProvider = optionsProvider;
        this.serverEvents = serverEvents;
    }

    /**
     * Deploys the {@link ServerVerticle verticles} to {@link Vertx}.
     *
     * @return a future completed with the result
     */
    public Future<String> start()
    {
        banner(System.out);
        validate();

        LOGGER.info("Starting Cassandra Sidecar");
        int serverVerticleCount = sidecarConfiguration.serviceConfiguration().serverVerticleInstances();
        Preconditions.checkArgument(serverVerticleCount > 0,
                                    "Server verticle count can not be less than 1");
        LOGGER.debug("Deploying {} verticles to vertx", serverVerticleCount);
        DeploymentOptions deploymentOptions = new DeploymentOptions().setInstances(serverVerticleCount);
        HttpServerOptions options = optionsProvider.apply(sidecarConfiguration);
        return vertx.deployVerticle(() -> {
                        ServerVerticle serverVerticle = new ServerVerticle(sidecarConfiguration, router, options);
                        deployedServerVerticles.add(serverVerticle);
                        return serverVerticle;
                    }, deploymentOptions)
                    .compose(this::notifyServerStart)
                    .onSuccess(s -> maybeStartPeriodicKeyStoreChangedCheckerTask());
    }

    /**
     * Undeploy a verticle deployment, stopping all the {@link ServerVerticle verticles}.
     *
     * @param deploymentId the deployment ID
     * @return a future completed with the result
     */
    public Future<Void> stop(String deploymentId)
    {
        LOGGER.info("Stopping Cassandra Sidecar");
        deployedServerVerticles.clear();
        Objects.requireNonNull(deploymentId, "deploymentId must not be null");
        return notifyServerStopping(deploymentId)
               .compose(v -> vertx.undeploy(deploymentId))
               .onSuccess(v -> LOGGER.info("Successfully stopped Cassandra Sidecar"));
    }

    /**
     * Stops the {@link Vertx} instance and release any resources held by it.
     *
     * <p>The instance cannot be used after it has been closed.
     *
     * @return a future completed with the result
     */
    public Future<CompositeFuture> close()
    {
        LOGGER.info("Stopping Cassandra Sidecar");
        deployedServerVerticles.clear();
        List<Future<Void>> closingFutures = new ArrayList<>();
        closingFutures.add(notifyServerStopping(null));

        Promise<Void> periodicTaskExecutorPromise = Promise.promise();
        periodicTaskExecutor.close(periodicTaskExecutorPromise);
        closingFutures.add(periodicTaskExecutorPromise.future());

        instancesConfig.instances()
                       .forEach(instance ->
                                closingFutures.add(executorPools.internal()
                                                                .executeBlocking(promise -> {
                                                                    instance.delegate().close();
                                                                    promise.complete(null);
                                                                })));
        closingFutures.add(executorPools.close());

        return Future.all(closingFutures)
                     .onComplete(v -> vertx.close())
                     .onSuccess(f -> LOGGER.info("Successfully stopped Cassandra Sidecar"));
    }

    public Future<CompositeFuture> updateSSLOptions()
    {
        SSLOptions options = new SSLOptions();
        // Sets the updated key store configuration to the SSLOptions object
        optionsProvider.configureKeyStore(options, sidecarConfiguration.sslConfiguration());
        // Sets the updated trust store configuration to the SSLOptions object
        optionsProvider.configureTrustStore(options, sidecarConfiguration.sslConfiguration());
        // Updates the SSL options of all the deployed verticles
        List<Future<CompositeFuture>> updateFutures =
        deployedServerVerticles.stream()
                               .map(serverVerticle -> serverVerticle.updateSSLOptions(options))
                               .collect(Collectors.toList());
        return Future.all(updateFutures);
    }

    /**
     * Expose the port of the first deployed verticle for testing purposes
     *
     * @return the port where the first verticle is deployed, or -1 if the server has not been deployed
     */
    @VisibleForTesting
    public int actualPort()
    {
        if (!deployedServerVerticles.isEmpty())
            return deployedServerVerticles.get(0).actualPort();
        return -1;
    }

    protected Future<String> notifyServerStart(String deploymentId)
    {
        LOGGER.info("Successfully started Cassandra Sidecar");
        vertx.eventBus().publish(serverEvents.ON_SERVER_START, deploymentId);
        return Future.succeededFuture(deploymentId);
    }

    protected Future<Void> notifyServerStopping(String deploymentId)
    {
        vertx.eventBus().publish(serverEvents.ON_SERVER_STOP, deploymentId);
        return Future.succeededFuture();
    }

    protected void banner(PrintStream out)
    {
        out.println(" _____                               _              _____ _     _                     \n" +
                    "/  __ \\                             | |            /  ___(_)   | |                    \n" +
                    "| /  \\/ __ _ ___ ___  __ _ _ __   __| |_ __ __ _   \\ `--. _  __| | ___  ___ __ _ _ __ \n" +
                    "| |    / _` / __/ __|/ _` | '_ \\ / _` | '__/ _` |   `--. \\ |/ _` |/ _ \\/ __/ _` | '__|\n" +
                    "| \\__/\\ (_| \\__ \\__ \\ (_| | | | | (_| | | | (_| |  /\\__/ / | (_| |  __/ (_| (_| | |   \n" +
                    " \\____/\\__,_|___/___/\\__,_|_| |_|\\__,_|_|  \\__,_|  \\____/|_|\\__,_|\\___|\\___\\__,_|_|\n" +
                    "                                                                                      \n" +
                    "                                                                                      ");
    }

    protected void validate()
    {
        SslConfiguration ssl = sidecarConfiguration.sslConfiguration();
        if (ssl == null || !ssl.enabled())
        {
            return;
        }

        try
        {
            if (!ssl.isKeystoreConfigured())
                throw new IllegalArgumentException("keyStorePath and keyStorePassword must be set if ssl enabled");

            SslUtils.validateSslOpts(ssl.keystore());

            if (ssl.isTrustStoreConfigured())
                SslUtils.validateSslOpts(ssl.truststore());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Invalid keystore parameters for SSL", e);
        }
    }

    /**
     * When the SSL configuration is provided and enabled it schedules a periodic task to check for changes
     * in the keystore file.
     */
    protected void maybeStartPeriodicKeyStoreChangedCheckerTask()
    {
        SslConfiguration ssl = sidecarConfiguration.sslConfiguration();
        if (ssl == null
            || !ssl.enabled()
            || !ssl.truststore().isConfigured()
            || !ssl.truststore().reloadStore())
        {
            return;
        }

        // the checks for the keystore changes are initialized here because we need a reference to the
        // server to be able to update the SSL options
        periodicTaskExecutor.schedule(new KeyStoreCheckPeriodicTask(vertx, this, ssl));
    }
}
