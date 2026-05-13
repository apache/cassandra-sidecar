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

import java.lang.reflect.Field;
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
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;


import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Token;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioners;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;

import org.apache.cassandra.sidecar.coordination.CassandraClientTokenRingProvider;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.jetbrains.annotations.NotNull;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
        mockHost("localhost1", "127.0.0.1", "-9223372036854775808", "DC1"),
        mockHost("localhost2", "127.0.0.2", "-8301034833169298228", "DC1"),
        mockHost("localhost3", "127.0.0.3", "-7378697629483820647", "DC1"),
        mockHost("localhost4", "127.0.0.4", "-6456360425798343066", "DC1"),
        mockHost("localhost5", "127.0.0.5", "-5534023222112865485", "DC1"),
        mockHost("localhost6", "127.0.0.6", "-4611686018427387904", "DC1"),
        mockHost("localhost7", "127.0.0.7", "-3689348814741910324", "DC1"),
        mockHost("localhost8", "127.0.0.8", "-2767011611056432743", "DC1"),
        mockHost("localhost9", "127.0.0.9", "-1844674407370955162", "DC1"),
        mockHost("localhost10", "127.0.0.10", "-922337203685477581", "DC1"),
        mockHost("localhost11", "127.0.0.11", "0", "DC1"),
        mockHost("localhost12", "127.0.0.12", "922337203685477580", "DC1"),
        mockHost("localhost13", "127.0.0.13", "1844674407370955161", "DC1"),
        mockHost("localhost14", "127.0.0.14", "2767011611056432742", "DC1"),
        mockHost("localhost15", "127.0.0.15", "3689348814741910323", "DC1"),
        mockHost("localhost16", "127.0.0.16", "4611686018427387904", "DC1"),
        mockHost("localhost17", "127.0.0.17", "5534023222112865484", "DC1"),
        mockHost("localhost18", "127.0.0.18", "6456360425798343065", "DC1"),
        mockHost("localhost19", "127.0.0.19", "7378697629483820646", "DC1"),
        mockHost("localhost20", "127.0.0.20", "8301034833169298227", "DC1"),
        mockHost("localhost21", "127.0.0.21", "-9223372036854775807", "DC2"),
        mockHost("localhost22", "127.0.0.22", "-8301034833169298227", "DC2"),
        mockHost("localhost23", "127.0.0.23", "-7378697629483820646", "DC2"),
        mockHost("localhost24", "127.0.0.24", "-6456360425798343065", "DC2"),
        mockHost("localhost25", "127.0.0.25", "-5534023222112865484", "DC2"),
        mockHost("localhost26", "127.0.0.26", "-4611686018427387903", "DC2"),
        mockHost("localhost27", "127.0.0.27", "-3689348814741910323", "DC2"),
        mockHost("localhost28", "127.0.0.28", "-2767011611056432742", "DC2"),
        mockHost("localhost29", "127.0.0.29", "-1844674407370955161", "DC2"),
        mockHost("localhost30", "127.0.0.30", "-922337203685477580", "DC2"),
        mockHost("localhost31", "127.0.0.31", "1", "DC2"),
        mockHost("localhost32", "127.0.0.32", "922337203685477581", "DC2"),
        mockHost("localhost33", "127.0.0.33", "1844674407370955162", "DC2"),
        mockHost("localhost34", "127.0.0.34", "2767011611056432743", "DC2"),
        mockHost("localhost35", "127.0.0.35", "3689348814741910324", "DC2"),
        mockHost("localhost36", "127.0.0.36", "4611686018427387905", "DC2"),
        mockHost("localhost37", "127.0.0.37", "5534023222112865485", "DC2"),
        mockHost("localhost38", "127.0.0.38", "6456360425798343066", "DC2"),
        mockHost("localhost39", "127.0.0.39", "7378697629483820647", "DC2"),
        mockHost("localhost40", "127.0.0.40", "8301034833169298228", "DC2")
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
                Token end = tokens.get(i);
                Token start;
                if (dc.equals("DC1"))
                {
                    start = tokens.get((i - 1 + tokens.size()) % tokens.size());
                }
                else
                {
                    // Handle the special case where prev() would fail for MIN_VALUE
                    MockToken mockEnd = (MockToken) end;
                    if (mockEnd.token == Long.MIN_VALUE)
                    {
                        // For MIN_VALUE, wrap around to MAX_VALUE
                        start = new MockToken(Long.MAX_VALUE);
                    }
                    else
                    {
                        start = mockEnd.prev();
                    }
                }

                // Create TokenRange with reflection-based mocking to handle final field
                TokenRange tokenRange = createMockTokenRange(start, end);
                result.add(tokenRange);
            }
        }
        when(metadata.getTokenRanges()).thenAnswer(invocation -> result);

        Map<String, Map<String, List<TokenRange>>> tokens = CassandraClientTokenRingProvider.assignedRangesOfAllInstancesByDc(dnsResolver, metadata);
        assertFalse(tokens.isEmpty());
        assertTrue(tokens.containsKey("DC1"));
        assertTrue(tokens.containsKey("DC2"));

        // DC1 should have zero '1-range' token ranges.
        List<TokenRange> dc1Ranges = tokens.get("DC1").values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        assertTrue(dc1Ranges.stream().allMatch(range -> {
            if (range.range.lowerEndpoint().toBigInteger().compareTo(BigInteger.valueOf(Long.parseLong("-9223372036854775808"))) == 0 &&
                range.range.lowerEndpoint().compareTo(range.range.upperEndpoint()) == 0)
            {
                return true;
            }
            return range.range.upperEndpoint().toBigInteger().subtract(range.range.lowerEndpoint().toBigInteger()).abs().compareTo(BigInteger.ONE) > 0;
        }));

        // DC2 is offset by 1 token so there will be 1 '1-range' token range at minToken
        List<TokenRange> dc2Ranges = tokens.get("DC2").values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<TokenRange> oneTokenRanges = dc2Ranges.stream().filter(range ->
                                                                           range.range.upperEndpoint().toBigInteger()
                                                                                .subtract(range.range.lowerEndpoint().toBigInteger())
                                                                                .abs()
                                                                                .compareTo(BigInteger.ONE) <= 0)
                                                          .collect(Collectors.toList());
        assertEquals(1, oneTokenRanges.size());
        TokenRange oneTokenRange = oneTokenRanges.get(0);
        assertEquals(BigInteger.valueOf(Long.MIN_VALUE), oneTokenRange.range.lowerEndpoint().toBigInteger());
        assertEquals(BigInteger.valueOf(Long.MIN_VALUE + 1), oneTokenRange.range.upperEndpoint().toBigInteger());
        assertTrue(dc2Ranges.stream().filter(f -> f != oneTokenRanges.get(0))
                            .allMatch(range -> range.range.upperEndpoint().toBigInteger()
                                                    .subtract(range.range.lowerEndpoint().toBigInteger())
                                                    .abs()
                                                    .compareTo(BigInteger.ONE) > 0));
    }

    public static Host mockHost(String node, String ip, String token, String dc)
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

    /**
     * Creates a properly mocked TokenRange using reflection to set the final range field.
     * This is necessary because TokenRange.range is a final field that can't be mocked normally.
     */
    private static TokenRange createMockTokenRange(Token start, Token end)
    {
        try
        {
            // Convert Datastax tokens to sidecar tokens
            org.apache.cassandra.sidecar.common.server.cluster.locator.Token sidecarStart =
                org.apache.cassandra.sidecar.common.server.cluster.locator.Token.from(((MockToken) start).token);
            org.apache.cassandra.sidecar.common.server.cluster.locator.Token sidecarEnd =
                org.apache.cassandra.sidecar.common.server.cluster.locator.Token.from(((MockToken) end).token);

            // Create mock sidecar tokens with proper behavior
            org.apache.cassandra.sidecar.common.server.cluster.locator.Token mockSidecarStart =
                mock(org.apache.cassandra.sidecar.common.server.cluster.locator.Token.class);
            org.apache.cassandra.sidecar.common.server.cluster.locator.Token mockSidecarEnd =
                mock(org.apache.cassandra.sidecar.common.server.cluster.locator.Token.class);

            // Configure mock token behavior for test assertions
            when(mockSidecarStart.toBigInteger()).thenReturn(sidecarStart.toBigInteger());
            when(mockSidecarEnd.toBigInteger()).thenReturn(sidecarEnd.toBigInteger());
            when(mockSidecarStart.compareTo(mockSidecarEnd)).thenReturn(sidecarStart.compareTo(sidecarEnd));
            when(mockSidecarStart.compareTo(any())).thenAnswer(invocation -> {
                org.apache.cassandra.sidecar.common.server.cluster.locator.Token other = invocation.getArgument(0);
                return sidecarStart.toBigInteger().compareTo(other.toBigInteger());
            });
            when(mockSidecarEnd.compareTo(any())).thenAnswer(invocation -> {
                org.apache.cassandra.sidecar.common.server.cluster.locator.Token other = invocation.getArgument(0);
                return sidecarEnd.toBigInteger().compareTo(other.toBigInteger());
            });

            // Create mock Range with proper endpoints
            @SuppressWarnings("unchecked")
            Range<org.apache.cassandra.sidecar.common.server.cluster.locator.Token> mockRange =
                (Range<org.apache.cassandra.sidecar.common.server.cluster.locator.Token>) mock(Range.class);

            when(mockRange.lowerEndpoint()).thenReturn(mockSidecarStart);
            when(mockRange.upperEndpoint()).thenReturn(mockSidecarEnd);

            // Create TokenRange instance and use reflection to set the final range field
            TokenRange tokenRange = mock(TokenRange.class);
            Field rangeField = TokenRange.class.getDeclaredField("range");
            rangeField.setAccessible(true);
            rangeField.set(tokenRange, mockRange);

            return tokenRange;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create mock TokenRange with reflection", e);
        }
    }

    public static DnsResolver mockDnsResolver()
    {

        Map<String, String> dnsMap = Map.of("localhost", "127.0.0.1",
                                            "localhost2", "127.0.0.2",
                                            "localhost3", "127.0.0.3"
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

        InstanceMetadata instance1 = getMockInstanceMetaData(101000101, "localhost", getMetadata());
        InstanceMetadata instance2 = getMockInstanceMetaData(101000201, "localhost2", getMetadata());
        InstanceMetadata instance3 = getMockInstanceMetaData(101000301, "localhost3", getMetadata());
        when(instancesMetadata.instances()).thenReturn(List.of(instance1, instance2, instance3));
        return instancesMetadata;
    }

    public static Metadata getMetadata()
    {
        Metadata metadata = mock(Metadata.class);
        when(metadata.getPartitioner()).thenReturn(Partitioners.MURMUR3.getClass().getSimpleName().toLowerCase());
        Set<Host> allHosts = Set.of(
        mockHost("localhost", "127.0.0.1", "-9223372036854775808", "DC1"),
        mockHost("localhost2", "127.0.0.2", "-8301034833169298228", "DC1"),
        mockHost("localhost3", "127.0.0.3", "-7378697629483820647", "DC1")
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
                                                                                      .hostId(UUID.randomUUID())
                                                                                      .build());
        when(instanceMetadata.delegate().version()).thenReturn(SimpleCassandraVersion.create("4.0.0.68"));
        when(instanceMetadata.delegate().metadata()).thenReturn(metadata);
        return instanceMetadata;
    }

    private InstanceMetadataFetcher mockInstanceMetadataFetcher()
    {
        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        Metadata metadata = getMetadata();
        when(fetcher.callOnFirstAvailableInstance(any())).thenReturn(metadata);
        return fetcher;
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
