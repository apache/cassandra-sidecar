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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.ActiveClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsNodeStateDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.ActiveClusterOpsSchema;
import org.apache.cassandra.sidecar.db.schema.ClusterOpsNodeStateSchema;
import org.apache.cassandra.sidecar.db.schema.ClusterOpsSchema;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.job.storage.CassandraStorageProvider;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.TableSchemaMapKeys;

/**
 * Guice module for operational job storage, providing schema registrations
 * and the {@link CassandraStorageProvider}.
 */
public class OperationalJobsModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.ClusterOpsSchemaKey.class)
    TableSchema clusterOpsSchema(SidecarConfiguration configuration)
    {
        return new ClusterOpsSchema(configuration.serviceConfiguration().schemaKeyspaceConfiguration(),
                                    configuration.operationalJobConfiguration().tablesTtl());
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.ClusterOpsNodeStateSchemaKey.class)
    TableSchema clusterOpsNodeStateSchema(SidecarConfiguration configuration)
    {
        return new ClusterOpsNodeStateSchema(configuration.serviceConfiguration().schemaKeyspaceConfiguration(),
                                             configuration.operationalJobConfiguration().tablesTtl());
    }

    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.ActiveClusterOpsSchemaKey.class)
    TableSchema activeClusterOpsSchema(SidecarConfiguration configuration)
    {
        return new ActiveClusterOpsSchema(configuration.serviceConfiguration().schemaKeyspaceConfiguration(),
                                          configuration.operationalJobConfiguration().tablesTtl());
    }

    @Provides
    @Singleton
    StorageProvider cassandraStorageProvider(CQLSessionProvider sessionProvider,
                                            ClusterOpsDatabaseAccessor clusterOpsAccessor,
                                            ClusterOpsNodeStateDatabaseAccessor nodeStateAccessor,
                                            ActiveClusterOpsDatabaseAccessor activeOpsAccessor)
    {
        return new CassandraStorageProvider(sessionProvider,
                                            clusterOpsAccessor, nodeStateAccessor, activeOpsAccessor, null);
    }
}
