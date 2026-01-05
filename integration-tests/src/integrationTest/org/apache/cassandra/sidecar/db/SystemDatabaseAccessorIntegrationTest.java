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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactoryImpl;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;
import org.apache.cassandra.sidecar.db.schema.SystemViewsSchema;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.SharedExecutorNettyOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.IClusterExtension;

import static org.apache.cassandra.sidecar.common.server.utils.ByteUtils.ONE_MIB;
import static org.apache.cassandra.testing.TlsTestUtils.waitForExistingRoles;
import static org.apache.cassandra.testing.TlsTestUtils.withAuthenticatedSession;
import static org.apache.cassandra.testing.utils.IInstanceUtils.buildContactList;
import static org.assertj.core.api.Assertions.assertThat;

class SystemDatabaseAccessorIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final long CDC_SIZE_LIMIT_IN_MIB = 5;

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .additionalInstanceConfig(Map.of("authenticator", "org.apache.cassandra.auth.PasswordAuthenticator",
                                                     "cdc_total_space_in_mb", CDC_SIZE_LIMIT_IN_MIB));
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster)
    {
        // DO NOTHING
    }

    @Test
    void testReadSystemSettings()
    {
        CQLSessionProvider sessionProvider = cqlSessionProvider(cluster);
        SystemViewsSchema systemViewsSchema = new SystemViewsSchema();
        systemViewsSchema.initialize(sessionProvider.get(), t -> false);
        SystemViewsDatabaseAccessor accessor = new SystemViewsDatabaseAccessor(systemViewsSchema, sessionProvider);
        Long cdcTotalSpaceSettings = accessor.cdcTotalSpaceBytesSetting();
        assertThat(cdcTotalSpaceSettings).isNotNull().isEqualTo(CDC_SIZE_LIMIT_IN_MIB * ONE_MIB);
    }

    @Test
    void testSystemAuthCrudOperations()
    {
        CQLSessionProvider sessionProvider = cqlSessionProvider(cluster);
        SystemAuthSchema systemAuthSchema = new SystemAuthSchema();
        systemAuthSchema.initialize(sessionProvider.get(), t -> false);
        SystemAuthDatabaseAccessor accessor = new SystemAuthDatabaseAccessor(systemAuthSchema, sessionProvider, new PermissionFactoryImpl());
        Map<String, Boolean> actualSuperUsers = accessor.findAllRolesToSuperuserStatus();
        assertThat(actualSuperUsers.size()).isEqualTo(3);
        assertThat(actualSuperUsers.get("super_user_role")).isTrue();
        assertThat(actualSuperUsers.get("non_super_user_role")).isTrue();
    }

    @Override
    protected void initializeSchemaForTest()
    {
        waitForExistingRoles(() -> withAuthenticatedSession(cluster.get(1), "cassandra", "cassandra", session ->
        {
            session.execute("CREATE ROLE IF NOT EXISTS \"super_user_role\" " +
                            "WITH SUPERUSER = true " +
                            "AND LOGIN = true");

            session.execute("CREATE ROLE IF NOT EXISTS \"non_super_user_role\" " +
                            "WITH LOGIN = true");

            session.execute("GRANT super_user_role TO non_super_user_role;");
        }, null));
    }

    private CQLSessionProvider cqlSessionProvider(IClusterExtension<? extends IInstance> cluster)
    {
        List<InetSocketAddress> address = buildContactList(cluster.stream().map(IInstance::config).collect(Collectors.toUnmodifiableList()));
        return new CQLSessionProviderImpl(address, address, 500, null, 0, "cassandra", "cassandra", null, SharedExecutorNettyOptions.INSTANCE);
    }
}
