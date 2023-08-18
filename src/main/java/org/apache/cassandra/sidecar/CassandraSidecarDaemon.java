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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.utils.SslUtils;

/**
 * Main class for initiating the Cassandra sidecar
 * Note: remember to start and stop all delegates of instances
 */
@Singleton
public class CassandraSidecarDaemon
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraSidecarDaemon.class);
    private final Vertx vertx;
    private final HttpServer server;
    private final SidecarConfiguration config;
    private final InstancesConfig instancesConfig;
    private final ExecutorPools executorPools;

    private long healthCheckTimerId;

    @Inject
    public CassandraSidecarDaemon(Vertx vertx,
                                  HttpServer server,
                                  SidecarConfiguration config,
                                  InstancesConfig instancesConfig,
                                  ExecutorPools executorPools)
    {
        this.vertx = vertx;
        this.server = server;
        this.config = config;
        this.executorPools = executorPools;
        this.instancesConfig = instancesConfig;
    }

    public static void main(String[] args)
    {
        CassandraSidecarDaemon app = Guice.createInjector(new MainModule())
                                          .getInstance(CassandraSidecarDaemon.class);

        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
    }

    public void start()
    {
        banner(System.out);
        validate();

        ServiceConfiguration service = config.serviceConfiguration();

        logger.info("Starting Cassandra Sidecar on {}:{}", service.host(), service.port());
        server.listen(service.port(), service.host())
              .onSuccess(p -> {
                  // Run a health check after start up
                  healthCheck();
                  // Configure the periodic timer to run subsequent health checks configured
                  // by the config.getHealthCheckFrequencyMillis() interval
                  updateHealthChecker(config.healthCheckConfiguration().checkIntervalMillis());
              });
    }

    public void stop()
    {
        logger.info("Stopping Cassandra Sidecar");
        List<Future> closingFutures = new ArrayList<>();
        closingFutures.add(server.close());
        executorPools.internal().cancelTimer(healthCheckTimerId);
        instancesConfig.instances()
                       .forEach(instance ->
                                closingFutures.add(executorPools.internal()
                                                                .executeBlocking(p -> instance.delegate().close())));

        try
        {
            // Some closing action is executed on the executorPool (which is closed when closing vertx).
            // Reflecting the dependncy below.

            CompositeFuture.all(closingFutures)
                           .onComplete(v -> vertx.close()).toCompletionStage()
                           .toCompletableFuture()
                           .get(10, TimeUnit.SECONDS);
            logger.info("Cassandra Sidecar stopped successfully");
        }
        catch (Exception ex)
        {
            logger.warn("Failed to stop Sidecar in 10 seconds", ex);
        }
    }

    private void banner(PrintStream out)
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

    private void validate()
    {
        SslConfiguration ssl = config.sslConfiguration();
        if (ssl != null && ssl.enabled())
        {
            try
            {
                if (!ssl.isKeystoreConfigured())
                    throw new IllegalArgumentException("keyStorePath and keyStorePassword must be set if ssl enabled");

                SslUtils.validateSslOpts(ssl.keystore().path(), ssl.keystore().password());

                if (ssl.isTruststoreConfigured())
                    SslUtils.validateSslOpts(ssl.truststore().path(), ssl.truststore().password());
            }
            catch (Exception e)
            {
                throw new RuntimeException("Invalid keystore parameters for SSL", e);
            }
        }
    }

    /**
     * Updates the health check frequency to the provided {@code healthCheckFrequencyMillis} value
     *
     * @param healthCheckFrequencyMillis the new health check frequency in milliseconds
     */
    public void updateHealthChecker(int healthCheckFrequencyMillis)
    {
        if (healthCheckTimerId > 0)
        {
            // Stop existing timer
            executorPools.internal().cancelTimer(healthCheckTimerId);
            logger.info("Stopped health check timer with timerId={}", healthCheckTimerId);
        }
        // TODO: when upgrading to latest vertx version, we can set an initial delay, and the periodic delay
        healthCheckTimerId = executorPools.internal()
                                          .setPeriodic(healthCheckFrequencyMillis, t -> healthCheck());
        logger.info("Started health check with frequency={} and timerId={}",
                    healthCheckFrequencyMillis, healthCheckTimerId);
    }

    /**
     * Checks the health of every instance configured in the {@link InstancesConfig}.
     * The health check is executed in a blocking thread to prevent the event-loop threads from blocking.
     */
    private void healthCheck()
    {
        instancesConfig.instances()
                       .forEach(instanceMetadata ->
                                executorPools.internal()
                                             .executeBlocking(promise -> instanceMetadata.delegate().healthCheck()));
    }
}

