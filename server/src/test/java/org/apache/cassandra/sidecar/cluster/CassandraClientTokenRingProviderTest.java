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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioners;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;

import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.coordination.CassandraClientTokenRingProvider;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;

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
                                                                                                            mockDnsResolver(),
                                                                                                            new DriverUtils());

    @Test
    public void testPrimaryRangesOfAllInstancesByDc()
    {
        DriverUtils driverUtils = new DriverUtils();
        Metadata metadata = mock(Metadata.class);
        TokenMap tokenMap = mock(TokenMap.class);
        when(tokenMap.getPartitionerName()).thenReturn(Partitioners.MURMUR3.name());
        when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
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
        Map<UUID, Node> allHosts = ImmutableMap.<UUID, Node>builder()
                                               .put(UUID.fromString("ea25ffc7-d403-402b-9cb3-587c133d70ec"),
                                                    mockHost("localhost1", "127.0.0.1", "-9223372036854775808", "DC1"))
                                               .put(UUID.fromString("e970f7b6-6e8a-46aa-9b84-2fe651a20719"),
                                                    mockHost("localhost2", "127.0.0.2", "-8301034833169298228", "DC1"))
                                               .put(UUID.fromString("de8e64f8-2b46-49bb-b67a-f73492ad03de"),
                                                    mockHost("localhost3", "127.0.0.3", "-7378697629483820647", "DC1"))
                                               .put(UUID.fromString("c1bc2650-fb11-4cf5-ac19-f70a9b6d1405"),
                                                    mockHost("localhost4", "127.0.0.4", "-6456360425798343066", "DC1"))
                                               .put(UUID.fromString("e71e12a9-602e-47c5-a115-164162987245"),
                                                    mockHost("localhost5", "127.0.0.5", "-5534023222112865485", "DC1"))
                                               .put(UUID.fromString("f3cf243d-ecd2-43da-8422-38de850501ad"),
                                                    mockHost("localhost6", "127.0.0.6", "-4611686018427387904", "DC1"))
                                               .put(UUID.fromString("4f0b650c-69c0-4441-a6f7-68da8ff074bd"),
                                                    mockHost("localhost7", "127.0.0.7", "-3689348814741910324", "DC1"))
                                               .put(UUID.fromString("395ca8fc-e964-41c1-b338-c5ed3efda1b7"),
                                                    mockHost("localhost8", "127.0.0.8", "-2767011611056432743", "DC1"))
                                               .put(UUID.fromString("3ef8fc97-f5f9-4323-8038-4d5584ba3785"),
                                                    mockHost("localhost9", "127.0.0.9", "-1844674407370955162", "DC1"))
                                               .put(UUID.fromString("e862d1f1-3ec3-40f1-b4ba-622ae0da26bf"),
                                                    mockHost("localhost10", "127.0.0.10", "-922337203685477581", "DC1"))
                                               .put(UUID.fromString("60796ffd-a023-477b-8a08-d81d2d42d327"),
                                                    mockHost("localhost11", "127.0.0.11", "0", "DC1"))
                                               .put(UUID.fromString("57fd2f22-14fb-457f-9545-ab93d5d608a4"),
                                                    mockHost("localhost12", "127.0.0.12", "922337203685477580", "DC1"))
                                               .put(UUID.fromString("a90bae6a-7ba8-4c32-8570-54570de720e4"),
                                                    mockHost("localhost13", "127.0.0.13", "1844674407370955161", "DC1"))
                                               .put(UUID.fromString("fddc822f-fbad-4050-916b-1021a4c27d2c"),
                                                    mockHost("localhost14", "127.0.0.14", "2767011611056432742", "DC1"))
                                               .put(UUID.fromString("df1bdb81-5472-432f-8340-41df4fff3d5d"),
                                                    mockHost("localhost15", "127.0.0.15", "3689348814741910323", "DC1"))
                                               .put(UUID.fromString("9148875c-1f6b-4310-82e0-fc369f8857b7"),
                                                    mockHost("localhost16", "127.0.0.16", "4611686018427387904", "DC1"))
                                               .put(UUID.fromString("69893624-32dd-4940-b929-45c619873823"),
                                                    mockHost("localhost17", "127.0.0.17", "5534023222112865484", "DC1"))
                                               .put(UUID.fromString("3ba1d0bc-e333-47da-b2f1-27ab248b6ddf"),
                                                    mockHost("localhost18", "127.0.0.18", "6456360425798343065", "DC1"))
                                               .put(UUID.fromString("3c4c4342-52bb-49f2-9e5b-2140cd1e5783"),
                                                    mockHost("localhost19", "127.0.0.19", "7378697629483820646", "DC1"))
                                               .put(UUID.fromString("178af717-ddf3-4e04-8828-a2b7ee65c6ac"),
                                                    mockHost("localhost20", "127.0.0.20", "8301034833169298227", "DC1"))
                                               .put(UUID.fromString("9bb8e91a-bd8c-4322-8784-bc30a3bcc884"),
                                                    mockHost("localhost21", "127.0.0.21", "-9223372036854775807", "DC2"))
                                               .put(UUID.fromString("a2a7373c-4f65-4604-90b0-9af4b77dfeca"),
                                                    mockHost("localhost22", "127.0.0.22", "-8301034833169298227", "DC2"))
                                               .put(UUID.fromString("81765e01-2e57-4432-b7a7-1832493216ef"),
                                                    mockHost("localhost23", "127.0.0.23", "-7378697629483820646", "DC2"))
                                               .put(UUID.fromString("556c9144-5545-4962-942d-336cc7d297ea"),
                                                    mockHost("localhost24", "127.0.0.24", "-6456360425798343065", "DC2"))
                                               .put(UUID.fromString("1ca09b9a-3609-48c5-978a-5d735b6e42bf"),
                                                    mockHost("localhost25", "127.0.0.25", "-5534023222112865484", "DC2"))
                                               .put(UUID.fromString("0232a64c-f470-4bcd-8e19-9c3a04cb9eec"),
                                                    mockHost("localhost26", "127.0.0.26", "-4611686018427387903", "DC2"))
                                               .put(UUID.fromString("4cd0c7c5-cd2c-4f3e-be73-d118769133e5"),
                                                    mockHost("localhost27", "127.0.0.27", "-3689348814741910323", "DC2"))
                                               .put(UUID.fromString("a79f23b3-f1f3-46d1-9756-55382b91d61a"),
                                                    mockHost("localhost28", "127.0.0.28", "-2767011611056432742", "DC2"))
                                               .put(UUID.fromString("75b4575d-1e99-4a6e-ab5a-9a9e160b25ba"),
                                                    mockHost("localhost29", "127.0.0.29", "-1844674407370955161", "DC2"))
                                               .put(UUID.fromString("26a2b85d-8c78-4668-8d60-941d65fec129"),
                                                    mockHost("localhost30", "127.0.0.30", "-922337203685477580", "DC2"))
                                               .put(UUID.fromString("58c67a76-3b48-4199-a2cf-e2debaaffa3e"),
                                                    mockHost("localhost31", "127.0.0.31", "1", "DC2"))
                                               .put(UUID.fromString("b830f9b7-6000-4e93-8b10-ed7c60b7552b"),
                                                    mockHost("localhost32", "127.0.0.32", "922337203685477581", "DC2"))
                                               .put(UUID.fromString("a83081ae-2866-4486-828a-196e792f096e"),
                                                    mockHost("localhost33", "127.0.0.33", "1844674407370955162", "DC2"))
                                               .put(UUID.fromString("674a1c95-901f-4345-a4e4-702c18421c7e"),
                                                    mockHost("localhost34", "127.0.0.34", "2767011611056432743", "DC2"))
                                               .put(UUID.fromString("a242e66d-4bd5-45aa-bd0e-acba91db20e7"),
                                                    mockHost("localhost35", "127.0.0.35", "3689348814741910324", "DC2"))
                                               .put(UUID.fromString("7235e1bc-448a-41d1-8e3d-a2f81c8895ea"),
                                                    mockHost("localhost36", "127.0.0.36", "4611686018427387905", "DC2"))
                                               .put(UUID.fromString("cb490c16-e9ab-480b-941b-a27a934ffdb1"),
                                                    mockHost("localhost37", "127.0.0.37", "5534023222112865485", "DC2"))
                                               .put(UUID.fromString("dd629f81-2798-4e58-b2ec-857ce0e02aa6"),
                                                    mockHost("localhost38", "127.0.0.38", "6456360425798343066", "DC2"))
                                               .put(UUID.fromString("8681e804-5d5c-4a66-8575-c62f161f7a80"),
                                                    mockHost("localhost38", "127.0.0.38", "6456360425798343066", "DC2"))
                                               .put(UUID.fromString("0fde1da0-ce7c-4ffd-be47-9fe7fd354da3"),
                                                    mockHost("localhost39", "127.0.0.39", "7378697629483820647", "DC2"))
                                               .put(UUID.fromString("2fbbc34e-6ad0-4d17-8d3c-ae48d907c427"),
                                                    mockHost("localhost40", "127.0.0.40", "8301034833169298228", "DC2"))
                                               .build();
        when(metadata.getNodes()).thenReturn(allHosts);

        Set<TokenRange> allTokenRange = new HashSet<>();
        Map<Node, Set<Token>> nodeTokens = allHosts.values().stream()
                                                   .map(n -> (DefaultNode) n)
                                                   .collect(Collectors.toMap(n -> n, n -> n.getRawTokens().stream()
                                                                                           .map(MockToken::new)
                                                                                           .collect(Collectors.toSet())));
        Map<String, List<Node>> hostByDc = allHosts.values().stream().collect(Collectors.groupingBy(Node::getDatacenter));
        for (Map.Entry<String, List<Node>> entry : hostByDc.entrySet())
        {
            String dc = entry.getKey();
            List<Token> tokens = hostByDc.get(dc).stream().map(n -> ((DefaultNode) n).getRawTokens()).flatMap(Collection::stream)
                                         .map(MockToken::new)
                                         .sorted(((Comparator<Token>) Comparable::compareTo).reversed())
                                         .collect(Collectors.toList());
            for (Node node : entry.getValue())
            {
                DefaultNode defaultNode = (DefaultNode) node;
                for (String rawToken : defaultNode.getRawTokens())
                {
                    nodeTokens.put(node, Set.of(new MockToken(rawToken)));
                }
            }
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
                    if (mockEnd.getValue() == Long.MIN_VALUE)
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
                allTokenRange.add(tokenRange);
            }
        }
        when(metadata.getTokenMap().get().getTokenRanges()).thenAnswer(invocation -> allTokenRange);
        when(metadata.getTokenMap().get().getTokens(any())).thenAnswer(invocation -> {
            Node n = invocation.getArgument(0);
            return nodeTokens.get(n);
        });

        Map<String, Map<String, List<TokenRange>>> tokens = CassandraClientTokenRingProvider
                                                            .assignedRangesOfAllInstancesByDc(dnsResolver, driverUtils, metadata);
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

    public static DefaultNode mockHost(String node, String ip, String token, String dc)
    {
        DefaultNode host = mock(DefaultNode.class, RETURNS_DEEP_STUBS);
        when(host.getRawTokens()).thenAnswer(invocation -> Set.of(token));
        when(host.getDatacenter()).thenReturn(dc);
        EndPoint endpoint = mock(EndPoint.class);
        InetAddress inetAddress = mock(InetAddress.class);
        when(inetAddress.getHostAddress()).thenReturn(ip);
        when(inetAddress.getHostName()).thenReturn(node);
        InetSocketAddress socketAddress = mock(InetSocketAddress.class);
        when(socketAddress.getHostName()).thenReturn(node);
        when(socketAddress.getAddress()).thenReturn(inetAddress);
        when(socketAddress.getPort()).thenReturn(9042);
        when(endpoint.resolve()).thenReturn(socketAddress);
        when(host.getEndPoint()).thenReturn(endpoint);
        when(host.toString()).thenReturn(node);
        return host;
    }

    @Test
    public void testLocalInstances()
    {
        Set<Node> localInstances = tokenRingProvider.localInstances();
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
                org.apache.cassandra.sidecar.common.server.cluster.locator.Token.from(((MockToken) start).getValue());
            org.apache.cassandra.sidecar.common.server.cluster.locator.Token sidecarEnd =
                org.apache.cassandra.sidecar.common.server.cluster.locator.Token.from(((MockToken) end).getValue());

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
        TokenMap tokenMap = mock(TokenMap.class);
        when(tokenMap.getPartitionerName()).thenReturn(Partitioners.MURMUR3.name());
        when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
        Map<UUID, Node> allHosts = Map.of(
        UUID.fromString("7091a44c-efc2-44c7-9834-12c2fa090d07"), mockHost("localhost", "127.0.0.1", "-9223372036854775808", "DC1"),
        UUID.fromString("cfba7f8b-0e4c-441f-91fb-6b05c2bc917a"), mockHost("localhost2", "127.0.0.2", "-8301034833169298228", "DC1"),
        UUID.fromString("3eeac2bd-b334-4b3d-a5a4-877d52c4e527"), mockHost("localhost3", "127.0.0.3", "-7378697629483820647", "DC1")
        );
        Map<Node, Set<Token>> nodeTokens = allHosts.values().stream()
                                                            .map(n -> (DefaultNode) n)
                                                            .collect(Collectors.toMap(n -> n, n -> n.getRawTokens().stream()
                                                                                                    .map(MockToken::new)
                                                                                                    .collect(Collectors.toSet())));
        when(metadata.getNodes()).thenReturn(allHosts);
        when(metadata.getTokenMap().get().getTokens(any())).thenAnswer(invocation -> {
            Node n = invocation.getArgument(0);
            return nodeTokens.get(n);
        });
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
        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        Metadata metadata = getMetadata();
        when(fetcher.callOnFirstAvailableInstance(any())).thenReturn(metadata);
        return fetcher;
    }

    private static class MockToken extends Murmur3Token
    {
        private MockToken(String token)
        {
            super(Long.parseLong(token));
        }

        private MockToken(long token)
        {
            super(token);
        }

        public MockToken prev()
        {
            if (getValue() == Long.MIN_VALUE)
            {
                throw new IllegalStateException();
            }
            return new MockToken(getValue() - 1);
        }

        public String toString()
        {
            return "MockToken{" +
                   "token=" + getValue() +
                   '}';
        }
    }
}
