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

package org.apache.cassandra.sidecar.cluster;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class InstancesMetadataImplTest
{
    static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    final AtomicReference<String> localhost1NewIp = new AtomicReference<>(null);
    final DnsResolver localhostResolver = new DnsResolver()
    {
        @Override
        public String resolve(String hostname)
        {
            String ipPrefix = "127.0.0.";
            if (hostname.startsWith("127."))
            {
                // it is ip address already
                return hostname;
            }

            String trimmed = hostname.replace("localhost", "");
            if (trimmed.isEmpty())
            {
                return ipPrefix + '1';
            }
            int digit = Integer.parseInt(trimmed);
            if (digit == 1 && localhost1NewIp.get() != null)
            {
                return localhost1NewIp.get();
            }
            return ipPrefix + digit;
        }

        @Override
        public String reverseResolve(String address)
        {
            return ""; // not examined in this test
        }
    };

    @TempDir
    Path tempDir;

    @Test
    void testLookupByHostName()
    {
        List<InstanceMetadata> instances = Arrays.asList(instance(1, "127.0.0.1"),
                                                         instance(2, "127.0.0.2"),
                                                         instance(3, "127.0.0.3"));
        InstancesMetadataImpl instancesMetadata = new InstancesMetadataImpl(instances, localhostResolver);
        assertThat(instancesMetadata.instanceFromHost("localhost").id()).isEqualTo(1);
        assertThat(instancesMetadata.instanceFromHost("localhost1").id()).isEqualTo(1);
        assertThat(instancesMetadata.instanceFromHost("localhost2").id()).isEqualTo(2);
        assertThat(instancesMetadata.instanceFromHost("localhost3").id()).isEqualTo(3);
        assertThat(instancesMetadata.instanceFromHost("127.0.0.1").id()).isEqualTo(1);
        assertThat(instancesMetadata.instanceFromHost("127.0.0.2").id()).isEqualTo(2);
        assertThat(instancesMetadata.instanceFromHost("127.0.0.3").id()).isEqualTo(3);

        assertThatThrownBy(() -> instancesMetadata.instanceFromHost("localhost999"))
        .isExactlyInstanceOf(NoSuchCassandraInstanceException.class)
        .hasMessage("Instance with host address 'localhost999' not found");
    }

    @Test
    void testLookupByIPAddress()
    {
        List<InstanceMetadata> instances = Arrays.asList(instance(1, "localhost1"),
                                                         instance(2, "localhost2"),
                                                         instance(3, "localhost3"));
        InstancesMetadataImpl instancesMetadata = new InstancesMetadataImpl(instances, localhostResolver);
        assertThat(instancesMetadata.instanceFromHost("localhost1").id()).isEqualTo(1);
        assertThat(instancesMetadata.instanceFromHost("localhost2").id()).isEqualTo(2);
        assertThat(instancesMetadata.instanceFromHost("localhost3").id()).isEqualTo(3);
        assertThat(instancesMetadata.instanceFromHost("127.0.0.1").id()).isEqualTo(1);
        assertThat(instancesMetadata.instanceFromHost("127.0.0.2").id()).isEqualTo(2);
        assertThat(instancesMetadata.instanceFromHost("127.0.0.3").id()).isEqualTo(3);

        String newIp = "127.1.2.3";
        assertThatThrownBy(() -> instancesMetadata.instanceFromHost(newIp))
        .isExactlyInstanceOf(NoSuchCassandraInstanceException.class)
        .hasMessage("Instance with host address '127.1.2.3' not found");

        localhost1NewIp.set(newIp);
        loopAssert(2, 100, () -> {
            // wait for the cache to be updated
            try
            {
                assertThat(instancesMetadata.instanceFromHost(newIp).id()).isEqualTo(1);
            }
            catch (NoSuchCassandraInstanceException e)
            {
                fail(e.getMessage());
                // continue
            }
        });
    }

    @Test
    void testLookupById()
    {
        List<InstanceMetadata> instances = Arrays.asList(instance(1, "localhost1"),
                                                         instance(2, "localhost2"),
                                                         instance(3, "localhost3"));
        InstancesMetadataImpl instancesMetadata = new InstancesMetadataImpl(instances, localhostResolver);
        assertThat(instancesMetadata.instanceFromId(1).host()).isEqualTo("localhost1");
        assertThat(instancesMetadata.instanceFromId(2).host()).isEqualTo("localhost2");
        assertThat(instancesMetadata.instanceFromId(3).host()).isEqualTo("localhost3");

        assertThatThrownBy(() -> instancesMetadata.instanceFromId(123))
        .isExactlyInstanceOf(NoSuchCassandraInstanceException.class)
        .hasMessage("Instance id '123' not found");
    }

    InstanceMetadata instance(int id, String hostNameOrIp)
    {
        String root = tempDir.toString();
        return InstanceMetadataImpl.builder()
                                   .id(id)
                                   .host(hostNameOrIp, localhostResolver)
                                   .port(9042)
                                   .storageDir(root)
                                   .metricRegistry(METRIC_REGISTRY)
                                   .build();
    }
}
