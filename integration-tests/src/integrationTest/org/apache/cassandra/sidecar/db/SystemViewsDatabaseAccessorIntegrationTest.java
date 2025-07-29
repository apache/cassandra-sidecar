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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SystemViewsSchema;
import org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.IClusterExtension;
import org.apache.cassandra.testing.IsolatedDTestClassLoaderWrapper;
import org.apache.cassandra.testing.TestVersion;

import static org.apache.cassandra.sidecar.common.server.utils.ByteUtils.ONE_MIB;
import static org.apache.cassandra.testing.utils.IInstanceUtils.buildContactList;
import static org.assertj.core.api.Assertions.assertThat;

class SystemViewsDatabaseAccessorIntegrationTest
{
    @ParameterizedTest(name = "{index} => version {0}")
    @MethodSource("org.apache.cassandra.testing.TestVersionSupplier#testVersions")
    void testReadSettings(TestVersion version) throws Exception
    {
        long cdcSizeLimitInMiB = 5;
        IsolatedDTestClassLoaderWrapper classLoaderWrapper = new IsolatedDTestClassLoaderWrapper();
        classLoaderWrapper.initializeDTestJarClassLoader(version, TestVersion.class);

        ClusterBuilderConfiguration testClusterConfiguration
        = new ClusterBuilderConfiguration().dynamicPortAllocation(true) // to allow parallel test runs
                                           .additionalInstanceConfig(Map.of("cdc_total_space_in_mb", cdcSizeLimitInMiB))
                                           .requestFeature(Feature.NATIVE_PROTOCOL);
        try (IClusterExtension<? extends IInstance> cluster = classLoaderWrapper.loadCluster(version.version(), testClusterConfiguration))
        {
            CQLSessionProvider sessionProvider = cqlSessionProvider(cluster);
            SystemViewsSchema systemViewsSchema = new SystemViewsSchema();
            systemViewsSchema.initialize(sessionProvider.get(), t -> false);
            SystemViewsDatabaseAccessor accessor = new SystemViewsDatabaseAccessor(systemViewsSchema, sessionProvider);
            Long cdcTotalSpaceSettings = accessor.cdcTotalSpaceBytesSetting();
            assertThat(cdcTotalSpaceSettings).isNotNull().isEqualTo(cdcSizeLimitInMiB * ONE_MIB);
        }
        finally
        {
            classLoaderWrapper.closeDTestJarClassLoader();
        }
    }

    private CQLSessionProvider cqlSessionProvider(IClusterExtension<? extends IInstance> cluster)
    {
        List<InetSocketAddress> address = buildContactList(cluster.stream().map(IInstance::config).collect(Collectors.toUnmodifiableList()));
        return new CQLSessionProviderImpl(address, address, 500, null, 0, SharedExecutorNettyOptions.INSTANCE);
    }
}
