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

package org.apache.cassandra.sidecar.cluster;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;


import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioners;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;

import org.apache.cassandra.sidecar.coordination.CassandraClientTokenRingProvider;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.jetbrains.annotations.NotNull;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for Cassandra Client token ring provider
 */
public class CassandraClientTokenRingProviderTest
{
    private final CassandraClientTokenRingProvider tokenRingProvider = new CassandraClientTokenRingProvider(mockInstancesMetadata(),
                                                                                                            mockInstanceMetadataFetcher(),
                                                                                                            mockDnsResolver());

    @Test
    public void testPrimaryRangesOfAllInstancesByDc()
    {
        Metadata metadata = mock(Metadata.class);
        when(metadata.getPartitioner()).thenReturn(Partitioners.MURMUR3.getClass().getSimpleName().toLowerCase());
        DnsResolver dnsResolver = new DnsResolver()
        {
            public String resolve(String s)
            {
                return "127.0.0." + s.substring(0, 5);
            }

            public String reverseResolve(String s)
            {
                return null;
            }
        };
        Set<Host> allHosts = Set.of(
        mockHost("local1", "127.0.0.1", "-9223372036854775808", "DC1"),
        mockHost("local2", "127.0.0.2", "-8301034833169298228", "DC1"),
        mockHost("local3", "127.0.0.3", "-7378697629483820647", "DC1"),
        mockHost("local4", "127.0.0.4", "-6456360425798343066", "DC1"),
        mockHost("local5", "127.0.0.5", "-5534023222112865485", "DC1"),
        mockHost("local6", "127.0.0.6", "-4611686018427387904", "DC1"),
        mockHost("local7", "127.0.0.7", "-3689348814741910324", "DC1"),
        mockHost("local8", "127.0.0.8", "-2767011611056432743", "DC1"),
        mockHost("local9", "127.0.0.9", "-1844674407370955162", "DC1"),
        mockHost("local10", "127.0.0.10", "-922337203685477581", "DC1"),
        mockHost("local11", "127.0.0.11", "0", "DC1"),
        mockHost("local12", "127.0.0.12", "922337203685477580", "DC1"),
        mockHost("local13", "127.0.0.13", "1844674407370955161", "DC1"),
        mockHost("local14", "127.0.0.14", "2767011611056432742", "DC1"),
        mockHost("local15", "127.0.0.15", "3689348814741910323", "DC1"),
        mockHost("local16", "127.0.0.16", "4611686018427387904", "DC1"),
        mockHost("local17", "127.0.0.17", "5534023222112865484", "DC1"),
        mockHost("local18", "127.0.0.18", "6456360425798343065", "DC1"),
        mockHost("local19", "127.0.0.19", "7378697629483820646", "DC1"),
        mockHost("local20", "127.0.0.20", "8301034833169298227", "DC1"),
        mockHost("local21", "127.0.0.21", "-9223372036854775807", "DC2"),
        mockHost("local22", "127.0.0.22", "-8301034833169298227", "DC2"),
        mockHost("local23", "127.0.0.23", "-7378697629483820646", "DC2"),
        mockHost("local24", "127.0.0.24", "-6456360425798343065", "DC2"),
        mockHost("local25", "127.0.0.25", "-5534023222112865484", "DC2"),
        mockHost("local26", "127.0.0.26", "-4611686018427387903", "DC2"),
        mockHost("local27", "127.0.0.27", "-3689348814741910323", "DC2"),
        mockHost("local28", "127.0.0.28", "-2767011611056432742", "DC2"),
        mockHost("local29", "127.0.0.29", "-1844674407370955161", "DC2"),
        mockHost("local30", "127.0.0.30", "-922337203685477580", "DC2"),
        mockHost("local31", "127.0.0.31", "1", "DC2"),
        mockHost("local32", "127.0.0.32", "922337203685477581", "DC2"),
        mockHost("local33", "127.0.0.33", "1844674407370955162", "DC2"),
        mockHost("local34", "127.0.0.34", "2767011611056432743", "DC2"),
        mockHost("local35", "127.0.0.35", "3689348814741910324", "DC2"),
        mockHost("local36", "127.0.0.36", "4611686018427387905", "DC2"),
        mockHost("local37", "127.0.0.37", "5534023222112865485", "DC2"),
        mockHost("local38", "127.0.0.38", "6456360425798343066", "DC2"),
        mockHost("local39", "127.0.0.39", "7378697629483820647", "DC2"),
        mockHost("local40", "127.0.0.40", "8301034833169298228", "DC2")
        );
        when(metadata.getAllHosts()).thenReturn(allHosts);

        Set<TokenRange> result = new HashSet<>();
        Map<String, List<Host>> hostByDc = allHosts.stream().collect(Collectors.groupingBy(Host::getDatacenter));
        for (Map.Entry<String, List<Host>> entry : hostByDc.entrySet())
        {
            String dc = entry.getKey();
            List<Token> tokens = hostByDc.get(dc).stream().map(Host::getTokens).flatMap(Collection::stream)
                                         .sorted(((Comparator<Token>) Comparable::compareTo).reversed())
                                         .collect(Collectors.toList());
            for (int i = 0; i < tokens.size(); i++)
            {
                TokenRange tokenRange = mock(TokenRange.class, RETURNS_DEEP_STUBS);
                Token end = tokens.get(i);
                Token start = dc.equals("DC1") ? tokens.get((i - 1 + tokens.size()) % tokens.size()) : ((MockToken) end).prev();
                when(tokenRange.getStart()).thenAnswer(invocation -> start);
                when(tokenRange.getEnd()).thenAnswer(invocation -> end);
                when(tokenRange.isWrappedAround()).thenReturn(start.compareTo(end) < 0);
                result.add(tokenRange);
            }
        }
        when(metadata.getTokenRanges()).thenAnswer(invocation -> result);

        Map<String, Map<String, List<Range<BigInteger>>>> tokens = CassandraClientTokenRingProvider.assignedRangesOfAllInstancesByDc(dnsResolver, metadata);
        assertFalse(tokens.isEmpty());
        assertTrue(tokens.containsKey("DC1"));
        assertTrue(tokens.containsKey("DC2"));

        // DC1 should have zero '1-range' token ranges.
        List<Range<BigInteger>> dc1Ranges = tokens.get("DC1").values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        assertTrue(dc1Ranges.stream().allMatch(range -> {
            if (range.lowerEndpoint().compareTo(BigInteger.valueOf(Long.parseLong("-9223372036854775808"))) == 0 &&
                range.lowerEndpoint().compareTo(range.upperEndpoint()) == 0)
            {
                return true;
            }
            return range.upperEndpoint().subtract(range.lowerEndpoint()).abs().compareTo(BigInteger.ONE) > 0;
        }));

        // DC2 is offset by 1 token so there will be 1 '1-range' token range at minToken
        List<Range<BigInteger>> dc2Ranges = tokens.get("DC2").values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<Range<BigInteger>> oneTokenRanges = dc2Ranges.stream().filter(range ->
                                                                           range.upperEndpoint()
                                                                                .subtract(range.lowerEndpoint())
                                                                                .abs()
                                                                                .compareTo(BigInteger.ONE) <= 0)
                                                          .collect(Collectors.toList());
        assertEquals(1, oneTokenRanges.size());
        assertEquals(Range.openClosed(BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MIN_VALUE + 1)), oneTokenRanges.get(0));
        assertTrue(dc2Ranges.stream().filter(f -> f != oneTokenRanges.get(0))
                            .allMatch(range -> range.upperEndpoint()
                                                    .subtract(range.lowerEndpoint())
                                                    .abs()
                                                    .compareTo(BigInteger.ONE) > 0));
    }

    private static Host mockHost(String node, String ip, String token, String dc)
    {
        Host host = mock(Host.class, RETURNS_DEEP_STUBS);
        when(host.getTokens()).thenAnswer(invocation -> Set.of(new MockToken(token)));
        when(host.getDatacenter()).thenReturn(dc);
        InetAddress addressMock = mock(InetAddress.class);
        when(addressMock.getHostAddress()).thenReturn(ip);
        when(addressMock.getHostName()).thenReturn(node);
        when(host.getAddress()).thenReturn(addressMock);
        return host;
    }

    @Test
    public void testLocalInstances()
    {
        Set<Host> localInstances = tokenRingProvider.localInstances();
        assertEquals(3, localInstances.size());
    }

    protected DnsResolver mockDnsResolver()
    {

        Map<String, String> dnsMap = Map.of("local1", "127.0.0.1",
                                            "local2", "127.0.0.2",
                                            "local13", "127.0.0.3"
                                            );
        DnsResolver dnsResolver = mock(DnsResolver.class);
        try
        {
            when(dnsResolver.resolve(anyString())).thenAnswer(invocation -> {
                String hostName = invocation.getArgument(0);
                return dnsMap.get(hostName);
            });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        return dnsResolver;
    }

    private InstancesMetadata mockInstancesMetadata()
    {
        InstancesMetadata instancesMetadata = mock(InstancesMetadata.class);

        InstanceMetadata instance1 = getMockInstanceMetaData(101000101, "local1", getMetadata());
        InstanceMetadata instance2 = getMockInstanceMetaData(101000201, "local2", getMetadata());
        InstanceMetadata instance3 = getMockInstanceMetaData(101000301, "local3", getMetadata());
        when(instancesMetadata.instances()).thenReturn(List.of(instance1, instance2, instance3));
        return instancesMetadata;
    }

    private Metadata getMetadata()
    {
        Metadata metadata = mock(Metadata.class);
        when(metadata.getPartitioner()).thenReturn(Partitioners.MURMUR3.getClass().getSimpleName().toLowerCase());
        Set<Host> allHosts = Set.of(
        mockHost("local1", "127.0.0.1", "-9223372036854775808", "DC1"),
        mockHost("local2", "127.0.0.2", "-8301034833169298228", "DC1"),
        mockHost("local3", "127.0.0.3", "-7378697629483820647", "DC1")
        );
        when(metadata.getAllHosts()).thenReturn(allHosts);
        return metadata;
    }

    public static InstanceMetadata getMockInstanceMetaData(int instanceId, String hostname, Metadata metadata)
    {
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class, RETURNS_DEEP_STUBS);
        when(instanceMetadata.id()).thenReturn(instanceId);
        when(instanceMetadata.host()).thenReturn(hostname);
        when(instanceMetadata.delegate().nodeSettings()).thenReturn(NodeSettings.builder()
                                                                                      .releaseVersion("4.0.0.68")
                                                                                      .partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
                                                                                      .sidecarVersion("1.0-TEST")
                                                                                      .datacenter("DC1")
                                                                                      .build());
        when(instanceMetadata.delegate().version()).thenReturn(SimpleCassandraVersion.create("4.0.0.68"));
        when(instanceMetadata.delegate().metadata()).thenReturn(metadata);
        return instanceMetadata;
    }



    private InstanceMetadataFetcher mockInstanceMetadataFetcher()
    {
        return mock(InstanceMetadataFetcher.class);
    }

    private static class MockToken extends Token
    {
        final Long token;

        private MockToken(String token)
        {
            this(Long.parseLong(token));
        }

        private MockToken(long token)
        {
            this.token = token;
        }

        @Override
        public DataType getType()
        {
            return DataType.bigint();
        }

        @Override
        public Object getValue()
        {
            return token;
        }

        @Override
        public ByteBuffer serialize(ProtocolVersion protocolVersion)
        {
            return null;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockToken mockToken = (MockToken) o;
            return Objects.equals(token, mockToken.token);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(token);
        }

        @Override
        public int compareTo(@NotNull Token o)
        {
            return ((MockToken) o).token.compareTo(token);
        }

        public MockToken prev()
        {
            if (token == Long.MIN_VALUE)
            {
                throw new IllegalStateException();
            }
            return new MockToken(token - 1);
        }

        public String toString()
        {
            return "MockToken{" +
                   "token=" + token +
                   '}';
        }
    }
}
