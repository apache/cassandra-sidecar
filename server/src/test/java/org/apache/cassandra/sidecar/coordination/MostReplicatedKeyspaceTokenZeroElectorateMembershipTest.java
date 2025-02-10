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

package org.apache.cassandra.sidecar.coordination;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MostReplicatedKeyspaceTokenZeroElectorateMembership}
 */
class MostReplicatedKeyspaceTokenZeroElectorateMembershipTest
{
    private static final SidecarConfigurationImpl CONFIG = new SidecarConfigurationImpl();

    @Test
    void testNoAvailableLocalInstances()
    {
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate = mock(CassandraAdapterDelegate.class);
        when(mockCassandraAdapterDelegate.localStorageBroadcastAddress())
        .thenThrow(new CassandraUnavailableException(CQL, "Cannot retrieve storageBroadcastAddress"));
        when(instanceMetadata.delegate()).thenReturn(mockCassandraAdapterDelegate);
        when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
        InstanceMetadataFetcher instanceMetadataFetcher = new InstanceMetadataFetcher(mockInstancesMetadata);
        ElectorateMembership membership = new MostReplicatedKeyspaceTokenZeroElectorateMembership(instanceMetadataFetcher, null, CONFIG);
        assertThat(membership.isMember()).as("When no local instances are managed by Sidecar, we can't determine participation")
                                         .isFalse();
    }

    @Test
    void testCqlSessionIsNotActive() throws UnknownHostException
    {
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate = mock(CassandraAdapterDelegate.class);
        when(mockCassandraAdapterDelegate.localStorageBroadcastAddress()).thenReturn(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8888));
        when(mockCassandraAdapterDelegate.storageOperations()).thenReturn(mock(StorageOperations.class));
        when(mockCassandraAdapterDelegate.nodeSettings()).thenReturn(mock(NodeSettings.class));
        when(instanceMetadata.delegate()).thenReturn(mockCassandraAdapterDelegate);
        when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
        InstanceMetadataFetcher instanceMetadataFetcher = new InstanceMetadataFetcher(mockInstancesMetadata);
        CQLSessionProvider mockCQLSessionProvider = mock(CQLSessionProvider.class);
        // the session is not available so we return null
        when(mockCQLSessionProvider.get()).thenThrow(new CassandraUnavailableException(CQL, new RuntimeException("connection failed")));
        ElectorateMembership membership = new MostReplicatedKeyspaceTokenZeroElectorateMembership(instanceMetadataFetcher, mockCQLSessionProvider, CONFIG);
        assertThat(membership.isMember()).as("When the CQL connection is unavailable, we can't determine participation")
                                         .isFalse();
    }
}
