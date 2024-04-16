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

package org.apache.cassandra.sidecar.tasks;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_START;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;

/**
 * Periodically checks the health of every instance configured in the {@link InstancesConfig}.
 */
public class HealthCheckPeriodicTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckPeriodicTask.class);
    private final EventBus eventBus;
    private final SidecarConfiguration configuration;
    private final InstancesConfig instancesConfig;
    private final TaskExecutorPool internalPool;
    private final SidecarMetrics metrics;

    public HealthCheckPeriodicTask(Vertx vertx,
                                   SidecarConfiguration configuration,
                                   InstancesConfig instancesConfig,
                                   ExecutorPools executorPools,
                                   SidecarMetrics metrics)
    {
        eventBus = vertx.eventBus();
        this.configuration = configuration;
        this.instancesConfig = instancesConfig;
        internalPool = executorPools.internal();
        this.metrics = metrics;
    }

    @Override
    public void registerPeriodicTaskExecutor(PeriodicTaskExecutor executor)
    {
        eventBus.localConsumer(ON_SERVER_START.address(), message -> executor.schedule(this));
        eventBus.localConsumer(ON_SERVER_STOP.address(), message -> executor.unschedule(this));
    }

    @Override
    public long initialDelay()
    {
        return configuration.healthCheckConfiguration().initialDelayMillis();
    }

    @Override
    public long delay()
    {
        return configuration.healthCheckConfiguration().checkIntervalMillis();
    }

    /**
     * Run health checks on all the configured instances
     */
    @Override
    public void execute(Promise<Void> promise)
    {
        AtomicInteger instanceDown = new AtomicInteger(0);
        List<Future<?>> futures = instancesConfig.instances()
                                                 .stream()
                                                 .map(instanceMetadata -> internalPool.executeBlocking(p -> {
                                                     try
                                                     {
                                                         instanceMetadata.delegate().healthCheck();
                                                         p.complete();
                                                     }
                                                     catch (Throwable cause)
                                                     {
                                                         instanceDown.incrementAndGet();
                                                         p.fail(cause);
                                                         LOGGER.error("Unable to complete health check on instance={}",
                                                                      instanceMetadata.id(), cause);
                                                     }
                                                 }, false))
                                                 .collect(Collectors.toList());

        // join always waits until all its futures are completed and will not fail as soon as one of the future fails
        Future.join(futures)
              .onComplete(v -> {
                  int instanceDownCount = instanceDown.get();
                  int instanceUpCount = instancesConfig.instances().size() - instanceDownCount;
                  metrics.server().health().cassandraInstancesUp.metric.setValue(instanceUpCount);
                  metrics.server().health().cassandraInstancesDown.metric.setValue(instanceDownCount);
              })
              .onSuccess(v -> promise.complete())
              .onFailure(promise::fail);
    }

    @Override
    public String name()
    {
        return "Health Check";
    }
}
