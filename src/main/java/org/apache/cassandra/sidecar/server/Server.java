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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SSLOptions;
import io.vertx.core.net.TrafficShapingOptions;
import io.vertx.ext.web.Router;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.tasks.HealthCheckPeriodicTask;
import org.apache.cassandra.sidecar.tasks.KeyStoreCheckPeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.RefreshPermissionCachesPeriodicTask;
import org.apache.cassandra.sidecar.utils.SslUtils;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_ALL_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;

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
    protected final PeriodicTaskExecutor periodicTaskExecutor;
    protected final HttpServerOptionsProvider optionsProvider;
    protected final SidecarMetrics metrics;
    protected final List<ServerVerticle> deployedServerVerticles = new CopyOnWriteArrayList<>();
    // Keeps track of all the Cassandra instance identifiers where CQL is ready
    private final Set<Integer> cqlReadyInstanceIds = Collections.synchronizedSet(new HashSet<>());
    private final RefreshPermissionCachesPeriodicTask refreshPermissionCachesPeriodicTask;

    @Inject
    public Server(Vertx vertx,
                  SidecarConfiguration sidecarConfiguration,
                  Router router,
                  InstancesConfig instancesConfig,
                  ExecutorPools executorPools,
                  PeriodicTaskExecutor periodicTaskExecutor,
                  HttpServerOptionsProvider optionsProvider,
                  SidecarMetrics metrics,
                  RefreshPermissionCachesPeriodicTask refreshPermissionCachesPeriodicTask)
    {
        this.vertx = vertx;
        this.executorPools = executorPools;
        this.sidecarConfiguration = sidecarConfiguration;
        this.instancesConfig = instancesConfig;
        this.router = router;
        this.periodicTaskExecutor = periodicTaskExecutor;
        this.optionsProvider = optionsProvider;
        this.metrics = metrics;
        this.refreshPermissionCachesPeriodicTask = refreshPermissionCachesPeriodicTask;
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
                    .compose(this::scheduleInternalPeriodicTasks)
                    .compose(this::notifyServerStart);
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
    public Future<Void> close()
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
                                                                .runBlocking(() -> instance.delegate().close())));
        return Future.all(closingFutures)
                     .compose(v1 -> executorPools.close())
                     .onComplete(v -> vertx.close())
                     .onSuccess(f -> LOGGER.info("Successfully stopped Cassandra Sidecar"));
    }

    /**
     * Updates the SSL Options for all servers in all the deployed verticle instances with the {@code timestamp}
     * of the updated file
     *
     * @param timestamp the timestamp of the updated file
     * @return a future to indicate the update was successfully completed
     */
    public Future<CompositeFuture> updateSSLOptions(long timestamp)
    {
        SSLOptions options = new SSLOptions();
        // Sets the updated SSL options
        optionsProvider.configureSSLOptions(options, sidecarConfiguration.sslConfiguration(), timestamp);
        // Updates the SSL options of all the deployed verticles
        List<Future<CompositeFuture>> updateFutures =
        deployedServerVerticles.stream()
                               .map(serverVerticle -> serverVerticle.updateSSLOptions(options))
                               .collect(Collectors.toList());
        return Future.all(updateFutures);
    }

    /**
     * Updates the traffic shaping options for all servers in all the deployed verticle instances
     *
     * @param options update traffic shaping options
     */
    public void updateTrafficShapingOptions(TrafficShapingOptions options)
    {
        // Updates the traffic shaping options of all the deployed verticles
        deployedServerVerticles.forEach(serverVerticle -> serverVerticle.updateTrafficShapingOptions(options));
    }

    /**
     * Expose the port of the first deployed verticle for testing purposes
     *
     * @return the port where the first verticle is deployed
     * @throws IllegalStateException if the server has not been deployed
     */
    @VisibleForTesting
    public int actualPort()
    {
        if (!deployedServerVerticles.isEmpty())
            return deployedServerVerticles.get(0).actualPort();
        throw new IllegalStateException("No deployed server verticles. Maybe server failed to deploy due to port conflict");
    }

    protected Future<String> notifyServerStart(String deploymentId)
    {
        LOGGER.info("Successfully started Cassandra Sidecar");
        vertx.eventBus().publish(SidecarServerEvents.ON_SERVER_START.address(), deploymentId);
        return Future.succeededFuture(deploymentId);
    }

    protected Future<Void> notifyServerStopping(String deploymentId)
    {
        vertx.eventBus().publish(SidecarServerEvents.ON_SERVER_STOP.address(), deploymentId);
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
     * Schedules internal {@link org.apache.cassandra.sidecar.tasks.PeriodicTask}s.
     *
     * @param deploymentId the deployment ID
     * @return a succeeded future with the deployment ID of the server
     */
    protected Future<String> scheduleInternalPeriodicTasks(String deploymentId)
    {
        periodicTaskExecutor.schedule(new HealthCheckPeriodicTask(vertx,
                                                                  sidecarConfiguration,
                                                                  instancesConfig,
                                                                  executorPools,
                                                                  metrics));
        maybeScheduleKeyStoreCheckPeriodicTask();

        MessageConsumer<JsonObject> cqlReadyConsumer = vertx.eventBus().localConsumer(ON_CASSANDRA_CQL_READY.address());
        cqlReadyConsumer.handler(message -> onCqlReady(cqlReadyConsumer, message));

        vertx.eventBus().localConsumer(ON_SIDECAR_SCHEMA_INITIALIZED.address(), h -> {
            periodicTaskExecutor.schedule(this.refreshPermissionCachesPeriodicTask);
        });

        return Future.succeededFuture(deploymentId);
    }

    /**
     * When the SSL configuration is provided and enabled it schedules a periodic task to check for changes
     * in the keystore file.
     */
    protected void maybeScheduleKeyStoreCheckPeriodicTask()
    {
        SslConfiguration ssl = sidecarConfiguration.sslConfiguration();
        if (ssl == null
            || !ssl.enabled()
            || !ssl.keystore().isConfigured()
            || !ssl.keystore().reloadStore())
        {
            return;
        }

        // the checks for the keystore changes are initialized here because we need a reference to the
        // server to be able to update the SSL options
        periodicTaskExecutor.schedule(new KeyStoreCheckPeriodicTask(vertx, this, ssl));
    }

    /**
     * Handles CQL ready events. When all the expected CQL connections are ready, notifies to the
     * {@link SidecarServerEvents#ON_ALL_CASSANDRA_CQL_READY} address.
     *
     * @param cqlReadyConsumer the consumer
     * @param message          the received message
     */
    protected void onCqlReady(MessageConsumer<JsonObject> cqlReadyConsumer, Message<JsonObject> message)
    {
        cqlReadyInstanceIds.add(message.body().getInteger("cassandraInstanceId"));

        boolean isCqlReadyOnAllInstances = instancesConfig.instances().stream()
                                                          .map(InstanceMetadata::id)
                                                          .allMatch(cqlReadyInstanceIds::contains);
        if (isCqlReadyOnAllInstances)
        {
            cqlReadyConsumer.unregister(); // stop listening to CQL ready events
            notifyAllCassandraCqlAreReady();
            LOGGER.info("CQL is ready for all Cassandra instances. {}", cqlReadyInstanceIds);
        }
    }

    /**
     * Constructs the notification message containing all the Cassandra instance IDs and publishes the message
     * notifying consumers that all the CQL connections are available.
     */
    protected void notifyAllCassandraCqlAreReady()
    {
        JsonArray cassandraInstanceIds = new JsonArray();
        cqlReadyInstanceIds.forEach(cassandraInstanceIds::add);
        JsonObject allReadyMessage = new JsonObject()
                                     .put("cassandraInstanceIds", cassandraInstanceIds);

        vertx.eventBus().publish(ON_ALL_CASSANDRA_CQL_READY.address(), allReadyMessage);
    }
}
