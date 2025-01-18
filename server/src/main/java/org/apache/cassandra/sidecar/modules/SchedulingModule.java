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

package org.apache.cassandra.sidecar.modules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.MultiBindingTypeResolver;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.tasks.KeyStoreCheckPeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

/**
 * Provides the scheduling capability in Sidecar. Periodic tasks are deployed to {@link PeriodicTaskExecutor}
 */
public class SchedulingModule extends AbstractModule
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulingModule.class);

    @Provides
    @Singleton
    public PeriodicTaskExecutor periodicTaskExecutor(Vertx vertx,
                                                     ExecutorPools executorPools,
                                                     ClusterLease clusterLease,
                                                     MultiBindingTypeResolver<PeriodicTask> resolver)
    {
        PeriodicTaskExecutor periodicTaskExecutor = new PeriodicTaskExecutor(executorPools, clusterLease);
        resolver.resolve().values().forEach(pt -> {
            LOGGER.debug("Deploying periodic task: {}", pt.getClass().getCanonicalName());
            try
            {
                pt.deploy(vertx, periodicTaskExecutor);
            }
            catch (Throwable cause)
            {
                throw new RuntimeException("Failed to deploy periodic task: " + pt.getClass().getCanonicalName(), cause);
            }
        });
        return periodicTaskExecutor;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.KeyStoreCheckPeriodicTaskKey.class)
    PeriodicTask keyStoreCheckPeriodicTask(Vertx vertx, Provider<Server> server, SidecarConfiguration configuration)
    {
        return new KeyStoreCheckPeriodicTask(vertx, server, configuration);
    }
}
