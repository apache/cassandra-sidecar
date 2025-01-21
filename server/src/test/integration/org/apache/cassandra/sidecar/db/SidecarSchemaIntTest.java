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

package org.apache.cassandra.sidecar.db;

import java.util.concurrent.TimeUnit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.CoordinationConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.CoordinationConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.PeriodicTaskConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ClusterLeaseClaimTask;
import org.apache.cassandra.sidecar.coordination.ElectorateMembership;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

class SidecarSchemaIntTest extends IntegrationTestBase
{
    @Override
    protected void beforeSetup()
    {
        installTestSpecificModule(new AbstractModule()
        {
            @Provides
            @Singleton
            public ClusterLease clusterLease()
            {
                // start with INDETERMINATE to compete for a leaseholder first, then init schema
                return new ClusterLease(ClusterLease.Ownership.INDETERMINATE);
            }

            @Provides
            @Singleton
            public CoordinationConfiguration clusterLeaseClaimTaskConfiguration()
            {
                // increase the claim frequency
                PeriodicTaskConfiguration taskConfig = new PeriodicTaskConfigurationImpl(true,
                                                                                         MillisecondBoundConfiguration.parse("1s"),
                                                                                         MillisecondBoundConfiguration.parse("1s"));
                return new CoordinationConfigurationImpl(taskConfig);
            }

            @Provides
            @Singleton
            public ClusterLeaseClaimTask clusterLeaseClaimTask(Vertx vertx,
                                                               ServiceConfiguration serviceConfiguration,
                                                               ElectorateMembership electorateMembership,
                                                               SidecarLeaseDatabaseAccessor accessor,
                                                               ClusterLease clusterLease,
                                                               SidecarMetrics metrics)
            {
                return new ClusterLeaseClaimTask(vertx,
                                                 serviceConfiguration,
                                                 electorateMembership,
                                                 accessor,
                                                 clusterLease,
                                                 metrics)
                {
                    @Override
                    public DurationSpec delay()
                    {
                        // ignore the minimum delay check that is coded in ClusterLeaseClaimTask
                        return MillisecondBoundConfiguration.parse("1s");
                    }
                };
            }
        });
    }

    @CassandraIntegrationTest
    void testSidecarSchemaInitializationFromBlank()
    {
        waitForSchemaReady(60, TimeUnit.SECONDS);
        SidecarSchema sidecarSchema = injector.getInstance(SidecarSchema.class);
        assertThat(sidecarSchema.isInitialized())
        .describedAs("SidecarSchema should be initialized")
        .isTrue();
        ClusterLease clusterLease = injector.getInstance(ClusterLease.class);
        assertThat(clusterLease.isClaimedByLocalSidecar())
        .describedAs("ClusterLease should be claimed by the local sidecar")
        .isTrue();
    }
}
