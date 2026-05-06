/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.cdc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SidecarReplicationFactorSupplierTest
{
    @Test
    void getReplicationFactorReturnsKeyspaceReplicationFromDriver()
    {
        Map<String, String> ntsRf = new HashMap<>();
        ntsRf.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        ntsRf.put("dca1", "3");
        ntsRf.put("phx1", "3");

        InstanceMetadataFetcher fetcher = stubFetcherForKeyspace("test_ks", ntsRf);
        SidecarReplicationFactorSupplier supplier = new SidecarReplicationFactorSupplier(fetcher);

        ReplicationFactor rf = supplier.getReplicationFactor("test_ks");

        assertThat(rf.getReplicationStrategy())
            .isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
        assertThat(rf.getOptions()).containsEntry("dca1", 3).containsEntry("phx1", 3);
        assertThat(rf.getTotalReplicationFactor()).isEqualTo(6);
    }

    @Test
    void getMaximalReplicationFactorPicksKeyspaceWithHighestTotalRf()
    {
        Map<String, String> smallRf = new HashMap<>();
        smallRf.put("class", "org.apache.cassandra.locator.SimpleStrategy");
        smallRf.put("replication_factor", "1");

        Map<String, String> bigRf = new HashMap<>();
        bigRf.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        bigRf.put("dca1", "3");
        bigRf.put("phx1", "3");

        KeyspaceMetadata systemKs = mock(KeyspaceMetadata.class);
        when(systemKs.getName()).thenReturn("system");
        when(systemKs.getReplication()).thenReturn(smallRf);

        KeyspaceMetadata testKs = mock(KeyspaceMetadata.class);
        when(testKs.getName()).thenReturn("test_ks");
        when(testKs.getReplication()).thenReturn(bigRf);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspaces()).thenReturn(Arrays.asList(systemKs, testKs));

        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        when(delegate.metadata()).thenReturn(metadata);

        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.delegate()).thenReturn(delegate);

        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        when(fetcher.callOnFirstAvailableInstance(any())).thenAnswer(inv -> {
            Function<InstanceMetadata, ?> fn = inv.getArgument(0);
            return fn.apply(instance);
        });

        SidecarReplicationFactorSupplier supplier = new SidecarReplicationFactorSupplier(fetcher);

        ReplicationFactor rf = supplier.getMaximalReplicationFactor();

        assertThat(rf.getTotalReplicationFactor()).isEqualTo(6);
        assertThat(rf.getReplicationStrategy())
            .isEqualTo(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy);
    }

    private InstanceMetadataFetcher stubFetcherForKeyspace(String keyspace, Map<String, String> replication)
    {
        KeyspaceMetadata ksMeta = mock(KeyspaceMetadata.class);
        when(ksMeta.getReplication()).thenReturn(replication);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspace(keyspace)).thenReturn(ksMeta);

        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        when(delegate.metadata()).thenReturn(metadata);

        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.delegate()).thenReturn(delegate);

        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        when(fetcher.callOnFirstAvailableInstance(any())).thenAnswer(inv -> {
            Function<InstanceMetadata, ?> fn = inv.getArgument(0);
            return fn.apply(instance);
        });
        return fetcher;
    }
}
