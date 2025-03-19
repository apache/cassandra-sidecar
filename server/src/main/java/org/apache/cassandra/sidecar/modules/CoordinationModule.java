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
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ClusterLeaseClaimTask;
import org.apache.cassandra.sidecar.coordination.ElectorateMembership;
import org.apache.cassandra.sidecar.db.SidecarLeaseDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarLeaseSchema;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.TableSchemaMapKeys;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Provides the best-effort single cluster leaseholder capability.
 * <p>It is suitable for use cases that desire single execution ideally, but can tolerate concurrent executions, across the cluster.
 * <p>For more details, see {@link ClusterLeaseClaimTask}
 */
public class CoordinationModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.SidecarLeaseSchemaKey.class)
    TableSchema sidecarLeaseSchema(ServiceConfiguration configuration)
    {
        return new SidecarLeaseSchema(configuration.schemaKeyspaceConfiguration());
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.ClusterLeaseClaimTaskKey.class)
    PeriodicTask clusterLeaseClaimTask(ServiceConfiguration serviceConfiguration,
                                       ElectorateMembership electorateMembership,
                                       SidecarLeaseDatabaseAccessor accessor,
                                       ClusterLease clusterLease,
                                       SidecarMetrics metrics)
    {
        return new ClusterLeaseClaimTask(serviceConfiguration, electorateMembership, accessor, clusterLease, metrics);
    }

    @Provides
    @Singleton
    ElectorateMembership electorateMembership(InstanceMetadataFetcher instanceMetadataFetcher,
                                              CQLSessionProvider cqlSessionProvider,
                                              SidecarConfiguration configuration)
    {
        return new ElectorateMembershipFactory().create(instanceMetadataFetcher, cqlSessionProvider, configuration);
    }
}
