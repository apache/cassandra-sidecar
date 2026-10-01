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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.adapters.base.CassandraStorageOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.ClusterMembershipJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.EndpointSnitchJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolvers;
import org.apache.cassandra.sidecar.common.server.utils.GossipInfoParser;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.adapters.base.jmx.ClusterMembershipJmxOperations.FAILURE_DETECTOR_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.jmx.EndpointSnitchJmxOperations.ENDPOINT_SNITCH_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the cluster lease electorate for nodes with IPV6 address.
 */
class ClusterLeaseElectorateIPv6Test
{
    private static final String DATACENTER = "datacenter1";
    private static final String PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
    private static final List<String> TOKEN_RANGE_OWNING_TOKEN_ZERO = Arrays.asList("-100", "100");

    static Stream<Arguments> clusters() throws UnknownHostException
    {
        return Stream.of(Arguments.of(new Fixture("IPv4",
                                                  new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 7000),
                                                  Arrays.asList("127.0.0.1:7000",
                                                                "127.0.0.2:7000",
                                                                "127.0.0.3:7000"))),
                         Arguments.of(new Fixture("IPv6",
                                                  new InetSocketAddress(InetAddress.getByName("2001:db8::1"), 7000),
                                                  Arrays.asList("[2001:db8:0:0:0:0:0:1]:7000",
                                                                "[2001:db8:0:0:0:0:0:2]:7000",
                                                                "[2001:db8:0:0:0:0:0:3]:7000"))));
    }

    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("clusters")
    void testGossipHostHeaderIsRecognized(Fixture fixture)
    {
        String header = "/" + fixture.endpoints.get(0);
        assertThat(GossipInfoParser.isGossipInfoHostHeader(header))
        .as("'%s' is a host header emitted by InetAddressAndPort#toString(true)", header)
        .isTrue();
    }

    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("clusters")
    void testIsMemberOfElectorateWhenOwningTokenZero(Fixture fixture)
    {
        SidecarConfiguration sidecarConfiguration = new SidecarConfigurationImpl();
        ElectorateMembership membership = electorateMembership(fixture, sidecarConfiguration);

        assertThat(membership.isMember())
        .as("The local instance owns token zero of %s, so it is part of the electorate",
            sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration().keyspace())
        .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "/[2001:db8::1]:7000",
        "/2001:db8::1",
        "/[2001:db8:0:0:0:0:0:1:7000",
        "/[2001:db8:0:0:0:1]:7000",
        "/[gggg:db8:0:0:0:0:0:1]:7000",
    })
    void testUnrecognizedIpv6HostHeaderIsRejected(String header)
    {
        assertThat(GossipInfoParser.isGossipInfoHostHeader(header))
        .as("'%s' is not a host header Cassandra emits", header)
        .isFalse();

        String newLine = System.lineSeparator();
        String rawGossipInfo = header + newLine + "  generation:1683666630" + newLine + "  heartbeat:275" + newLine;
        assertThatThrownBy(() -> GossipInfoParser.parse(rawGossipInfo))
        .as("Parsing must fail loudly instead of misreading the fields of an unrecognized host")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(header);
    }

    private ElectorateMembership electorateMembership(Fixture fixture, SidecarConfiguration sidecarConfiguration)
    {
        String keyspace = sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration().keyspace();

        NodeSettings nodeSettings = mock(NodeSettings.class);
        when(nodeSettings.partitioner()).thenReturn(PARTITIONER);

        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        when(delegate.localStorageBroadcastAddress()).thenReturn(fixture.localBroadcastAddress);
        when(delegate.nodeSettings()).thenReturn(nodeSettings);
        StorageOperations storageOperations = new CassandraStorageOperations(jmxClient(fixture.endpoints, keyspace),
                                                                            DnsResolvers.RESOLVE_TO_IP);
        when(delegate.storageOperations()).thenReturn(storageOperations);

        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        when(instanceMetadata.delegate()).thenReturn(delegate);
        InstancesMetadata instancesMetadata = mock(InstancesMetadata.class);
        when(instancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));

        return new SidecarInternalTokenZeroElectorateMembership(new InstanceMetadataFetcher(instancesMetadata),
                                                                sidecarConfiguration);
    }

    private static JmxClient jmxClient(List<String> endpoints, String keyspace)
    {
        Map<List<String>, List<String>> rangeToEndpoints = new HashMap<>();
        rangeToEndpoints.put(TOKEN_RANGE_OWNING_TOKEN_ZERO, endpoints);

        StorageJmxOperations storage = mock(StorageJmxOperations.class);
        when(storage.isGossipRunning()).thenReturn(true);
        when(storage.getRangeToEndpointWithPortMap(keyspace)).thenReturn(rangeToEndpoints);
        when(storage.getPendingRangeToEndpointWithPortMap(keyspace)).thenReturn(Collections.emptyMap());
        when(storage.getLiveNodesWithPort()).thenReturn(endpoints);
        when(storage.getUnreachableNodesWithPort()).thenReturn(Collections.emptyList());
        when(storage.getJoiningNodesWithPort()).thenReturn(Collections.emptyList());
        when(storage.getLeavingNodesWithPort()).thenReturn(Collections.emptyList());
        when(storage.getMovingNodesWithPort()).thenReturn(Collections.emptyList());

        EndpointSnitchJmxOperations endpointSnitch = mock(EndpointSnitchJmxOperations.class);
        try
        {
            when(endpointSnitch.getDatacenter(anyString())).thenReturn(DATACENTER);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e); // stubbing only, cannot be thrown
        }

        ClusterMembershipJmxOperations clusterMembership = mock(ClusterMembershipJmxOperations.class);
        when(clusterMembership.getAllEndpointStatesWithPort()).thenReturn(rawGossipInfo(endpoints));

        JmxClient jmxClient = mock(JmxClient.class);
        when(jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)).thenReturn(storage);
        when(jmxClient.proxy(EndpointSnitchJmxOperations.class, ENDPOINT_SNITCH_INFO_OBJ_NAME)).thenReturn(endpointSnitch);
        when(jmxClient.proxy(ClusterMembershipJmxOperations.class, FAILURE_DETECTOR_OBJ_NAME)).thenReturn(clusterMembership);
        return jmxClient;
    }

    private static String rawGossipInfo(List<String> endpoints)
    {
        String newLine = System.lineSeparator();
        StringBuilder sb = new StringBuilder();
        for (String endpoint : endpoints)
        {
            sb.append('/').append(endpoint).append(newLine)
              .append("  generation:1683666630").append(newLine)
              .append("  heartbeat:275").append(newLine)
              .append("  LOAD:210:88971.0").append(newLine)
              .append("  DC:8:").append(DATACENTER).append(newLine)
              .append("  RACK:10:rack1").append(newLine)
              .append("  STATUS_WITH_PORT:16:NORMAL,-1").append(newLine)
              .append("  SSTABLE_VERSIONS:6:big-nb").append(newLine)
              .append("  TOKENS:15:<hidden>").append(newLine);
        }
        return sb.toString();
    }

    static class Fixture
    {
        final String name;
        final InetSocketAddress localBroadcastAddress;
        final List<String> endpoints;

        Fixture(String name, InetSocketAddress localBroadcastAddress, List<String> endpoints)
        {
            this.name = name;
            this.localBroadcastAddress = localBroadcastAddress;
            this.endpoints = endpoints;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
