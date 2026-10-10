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

package org.apache.cassandra.sidecar.job.storage;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.ActiveClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsDatabaseAccessor;
import org.apache.cassandra.sidecar.db.ClusterOpsNodeStateDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL_AND_JMX;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CassandraStorageProvider}
 */
class CassandraStorageProviderTest
{
    private static final String CLUSTER_NAME = "test-cluster";
    private static final UUID OPERATION_ID = UUIDs.timeBased();

    private ActiveClusterOpsDatabaseAccessor activeOpsAccessor;
    private InstanceMetadataFetcher instanceMetadataFetcher;
    private CassandraStorageProvider provider;

    @BeforeEach
    void setup()
    {
        activeOpsAccessor = mock(ActiveClusterOpsDatabaseAccessor.class);
        instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        provider = new CassandraStorageProvider(mock(CQLSessionProvider.class),
                                                mock(ClusterOpsDatabaseAccessor.class),
                                                mock(ClusterOpsNodeStateDatabaseAccessor.class),
                                                activeOpsAccessor,
                                                instanceMetadataFetcher,
                                                CLUSTER_NAME);
    }

    @Test
    void testResolvesDatacenterFromNodeSettings()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenReturn(NodeSettings.builder().datacenter("dc-from-node").build());

        provider.trySetActiveOperation(OperationType.DRAIN, OPERATION_ID, null);

        verify(activeOpsAccessor).trySetActiveOperation(eq(CLUSTER_NAME), eq("dc-from-node"),
                                                        eq(OperationType.DRAIN), eq(OPERATION_ID));
    }

    @Test
    void testThrowsWhenNodeSettingsUnavailable()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenThrow(new CassandraUnavailableException(CQL_AND_JMX, "node settings unavailable"));

        assertThatThrownBy(() -> provider.trySetActiveOperation(OperationType.DRAIN, OPERATION_ID, null))
        .isInstanceOf(StorageProviderException.class)
        .hasMessageContaining("Failed to initialize storage provider");
    }

    @Test
    void testThrowsWhenNodeSettingsDatacenterMissing()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenReturn(NodeSettings.builder().build());

        assertThatThrownBy(() -> provider.trySetActiveOperation(OperationType.DRAIN, OPERATION_ID, null))
        .isInstanceOf(StorageProviderException.class)
        .hasMessageContaining("Failed to resolve local datacenter");
    }

    @Test
    void testExplicitTargetDatacenterOverridesResolvedLocalDatacenter()
    {
        when(instanceMetadataFetcher.<NodeSettings>callOnFirstAvailableInstance(any()))
        .thenReturn(NodeSettings.builder().datacenter("dc-from-node").build());

        provider.trySetActiveOperation(OperationType.DRAIN, OPERATION_ID, "explicit-dc");

        verify(activeOpsAccessor).trySetActiveOperation(eq(CLUSTER_NAME), eq("explicit-dc"),
                                                        eq(OperationType.DRAIN), eq(OPERATION_ID));
    }
}
