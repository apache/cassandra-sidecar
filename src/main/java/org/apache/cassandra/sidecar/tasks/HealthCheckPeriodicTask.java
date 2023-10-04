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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Periodically checks the health of every instance configured in the {@link InstancesConfig}.
 */
@Singleton
public class HealthCheckPeriodicTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckPeriodicTask.class);
    private final SidecarConfiguration configuration;
    private final InstancesConfig instancesConfig;
    private final ExecutorPools.TaskExecutorPool internalPool;

    @Inject
    public HealthCheckPeriodicTask(SidecarConfiguration configuration,
                                   InstancesConfig instancesConfig,
                                   ExecutorPools executorPools)
    {
        this.configuration = configuration;
        this.instancesConfig = instancesConfig;
        internalPool = executorPools.internal();
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
    public void execute()
    {
        instancesConfig.instances()
                       .forEach(instanceMetadata -> internalPool.executeBlocking(promise -> {
                           try
                           {
                               instanceMetadata.delegate().healthCheck();
                               promise.complete();
                           }
                           catch (Throwable cause)
                           {
                               promise.fail(cause);
                               LOGGER.error("Unable to complete health check on instance={}",
                                            instanceMetadata.id(), cause);
                           }
                       }));
    }

    @Override
    public String name()
    {
        return "Health Check";
    }
}
