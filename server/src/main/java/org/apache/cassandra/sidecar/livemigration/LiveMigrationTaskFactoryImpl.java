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

package org.apache.cassandra.sidecar.livemigration;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;

/**
 * Factory implementation which produces {@link LiveMigrationTask} instances.
 */
@Singleton
public class LiveMigrationTaskFactoryImpl implements LiveMigrationTaskFactory
{

    private final Vertx vertx;
    private final SidecarClientProvider sidecarClientProvider;
    private final LiveMigrationConfiguration liveMigrationConfiguration;
    private final ExecutorPools executorPools;

    @Inject
    public LiveMigrationTaskFactoryImpl(Vertx vertx,
                                        ExecutorPools executorPools,
                                        SidecarClientProvider sidecarClientProvider,
                                        SidecarConfiguration sidecarConfiguration)
    {
        this.vertx = vertx;
        this.executorPools = executorPools;
        this.sidecarClientProvider = sidecarClientProvider;
        this.liveMigrationConfiguration = sidecarConfiguration.liveMigrationConfiguration();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LiveMigrationTask create(String id,
                                    LiveMigrationDataCopyRequest request,
                                    String source,
                                    int port,
                                    InstanceMetadata instanceMetadata)
    {
        return new LiveMigrationTaskImpl(vertx, executorPools, sidecarClientProvider, liveMigrationConfiguration,
                                         id, request, source, port, instanceMetadata);
    }
}
