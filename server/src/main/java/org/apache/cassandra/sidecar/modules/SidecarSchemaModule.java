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
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.db.schema.SidecarInternalKeyspace;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchemaInitializer;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.MultiBindingTypeResolver;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;

/**
 * Provides the capability of sidecar internal tables for internal data persistence
 */
public class SidecarSchemaModule extends AbstractModule
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarSchemaModule.class);

    @Provides
    @Singleton
    public SidecarSchema sidecarSchema(Vertx vertx,
                                       SidecarConfiguration configuration,
                                       MultiBindingTypeResolver<TableSchema> resolver)
    {
        SidecarInternalKeyspace sidecarInternalKeyspace = new SidecarInternalKeyspace(configuration);
        // register table schema when enabled
        resolver.resolve().values().forEach(tableSchema -> {
            LOGGER.info("Registering table schema: {}", tableSchema);
            try
            {
                sidecarInternalKeyspace.registerTableSchema(tableSchema);
            }
            catch (Throwable cause)
            {
                throw new RuntimeException("Failed to register table schema: " + tableSchema, cause);
            }
        });
        return new SidecarSchema(vertx, configuration, sidecarInternalKeyspace);
    }

    @Provides
    @Singleton
    public TableSchemaFetcher tableSchemaFetcher(SidecarSchema sidecarSchema)
    {
        return sidecarSchema;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.SidecarSchemaInitializerTaskKey.class)
    PeriodicTask sidecarSchemaInitializer(SidecarConfiguration configuration,
                                          CQLSessionProvider cqlSessionProvider,
                                          SidecarMetrics sidecarMetrics,
                                          SidecarSchema sidecarSchema,
                                          ClusterLease clusterLease)
    {
        return new SidecarSchemaInitializer(configuration,
                                            cqlSessionProvider,
                                            sidecarSchema.sidecarInternalKeyspace(),
                                            sidecarMetrics.server().schema(),
                                            clusterLease);
    }
}
