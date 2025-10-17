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

package org.apache.cassandra.sidecar.lifecycle;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests the {@link InJvmDTestLifecycleProvider} to ensure it can start and stop a Cassandra node
 */
public class InJvmLifecycleProviderIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    static final InstanceMetadata LOCALHOST_METADATA = mock(InstanceMetadata.class);
    private static final String JVM_LIFECYCLE_TEST_MIN_VERSION = "4.1";

    @BeforeAll
    static void beforeAll()
    {
        when(LOCALHOST_METADATA.host()).thenReturn("localhost");
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().startCluster(false);
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        // JVM Distributed Test framework contains a bug with restarting nodes in version 4.0 (CASSANDRA-19729)
        assumeThat(SimpleCassandraVersion.create(testVersion.version()))
        .as("JVM Distributed Test framework contains a bug with restarting nodes in version 4.0 (CASSANDRA-19729)")
        .isGreaterThanOrEqualTo(SimpleCassandraVersion.create(JVM_LIFECYCLE_TEST_MIN_VERSION));
    }

    @Test
    void testInJvmLifecycleProviderStartAndStopAndRecoveryAfterCrash() throws Exception
    {
        // Simulate node crashing by directly calling the lifecycle provider's stop method
        Runnable cassandraCrasher = () -> {
            LifecycleProvider lifecycleProvider = serverWrapper.injector.getInstance(LifecycleProvider.class);
            lifecycleProvider.stop(LOCALHOST_METADATA);
        };

        LifecycleProviderIntegrationTester tester = new LifecycleProviderIntegrationTester(
                trustedClient(),
                LOCALHOST_METADATA.host(),
                serverWrapper.serverPort,
                cassandraCrasher);

        tester.testLifecycleProviderStartAndStopAndRecoveryAfterCrash();
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Do nothing
    }
}
