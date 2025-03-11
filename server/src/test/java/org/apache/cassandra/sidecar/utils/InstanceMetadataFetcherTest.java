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

package org.apache.cassandra.sidecar.utils;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadataImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class InstanceMetadataFetcherTest
{
    @TempDir
    Path tempDir;

    @Test
    void testCallOnFirstAvailableInstance()
    {
        List<InstanceMetadata> instances = Arrays.asList(instance(1, "127.0.0.1", false),
                                                         instance(2, "127.0.0.2", true),
                                                         instance(3, "127.0.0.3", true));
        InstancesMetadataImpl instancesMetadata = new InstancesMetadataImpl(instances, DnsResolver.DEFAULT);
        InstanceMetadataFetcher fetcher = new InstanceMetadataFetcher(instancesMetadata);
        CassandraAdapterDelegate delegate = fetcher.callOnFirstAvailableInstance(InstanceMetadata::delegate);
        assertThat(delegate)
        .describedAs("The delegate of instance 2 should be returned")
        .isNotNull()
        .isSameAs(instances.get(1).delegate());
    }

    @Test
    void testCallOnFirstAvailableInstanceExhausts()
    {
        List<InstanceMetadata> instances = Arrays.asList(instance(1, "127.0.0.1", false),
                                                         instance(2, "127.0.0.2", false));
        InstancesMetadataImpl instancesMetadata = new InstancesMetadataImpl(instances, DnsResolver.DEFAULT);
        InstanceMetadataFetcher fetcher = new InstanceMetadataFetcher(instancesMetadata);
        assertThatThrownBy(() -> fetcher.callOnFirstAvailableInstance(InstanceMetadata::delegate))
        .isExactlyInstanceOf(CassandraUnavailableException.class)
        .hasMessageContaining("All local Cassandra nodes are exhausted. But none is available");
    }

    private InstanceMetadata instance(int id, String host, boolean isAvailable)
    {
        InstanceMetadataImpl.Builder builder = InstanceMetadataImpl.builder()
                                                                   .id(id)
                                                                   .host(host, DnsResolver.DEFAULT)
                                                                   .port(9042)
                                                                   .storageDir(tempDir.toString())
                                                                   .metricRegistry(new MetricRegistry());
        if (isAvailable)
        {
            builder.delegate(mock(CassandraAdapterDelegate.class));
        }
        return builder.build();
    }
}
