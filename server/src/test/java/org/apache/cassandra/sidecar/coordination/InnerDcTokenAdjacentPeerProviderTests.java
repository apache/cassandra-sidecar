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

package org.apache.cassandra.sidecar.coordination;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for InnerDcTokenAdjacentPeerProvider
 */
public class InnerDcTokenAdjacentPeerProviderTests
{
    private static final List<BigInteger> TOKENS = Stream.of(
                                                         "-9223372036854775808",
                                                         "-8070450532247928832",
                                                         "-6917529027641081856",
                                                         "-5764607523034234880",
                                                         "-4611686018427387904",
                                                         "-3458764513820540928",
                                                         "-2305843009213693952",
                                                         "-1152921504606846976",
                                                         "0",
                                                         "1152921504606846976",
                                                         "2305843009213693952",
                                                         "3458764513820540928",
                                                         "4611686018427387904",
                                                         "5764607523034234880",
                                                         "6917529027641081856",
                                                         "8070450532247928832"
                                                         )
                                                         .map(BigInteger::new)
                                                         .collect(Collectors.toList());
    private static final int NUM_INSTANCES_PER_HOST = 4;
    private static final List<String> INSTANCES = IntStream.range(0, TOKENS.size())
                                                           .mapToObj(i -> String.format("host%d-i%d",
                                                                                        (i % NUM_INSTANCES_PER_HOST) + 1,
                                                                                        1 + (int) ((i / (double) NUM_INSTANCES_PER_HOST))))
                                                           .collect(Collectors.toList());

    @Test
    public void testInnerDcTokenAdjacentBuddyProvider()
    {
        InstancesMetadata instancesMetadata = mock(InstancesMetadata.class);
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
        when(serviceConfiguration.port()).thenReturn(9043);

        Metadata metadata = mock(Metadata.class);
        List<KeyspaceMetadata> keyspaces = List.of(mockKeyspace("ks1", Map.of("DC1", "3", "DC2", "3")),
                                                   mockKeyspace("ks2", Map.of("DC2", "5")),
                                                   mockKeyspace("ks3", Map.of("DC1", "3")));
        when(metadata.getKeyspaces()).thenReturn(keyspaces);
        Set<Host> allHosts = IntStream.range(0, INSTANCES.size())
                                      .mapToObj(i -> List.of(mockHost("dc1-" + INSTANCES.get(i), TOKENS.get(i), "DC1"),
                                                             mockHost("dc2-" + INSTANCES.get(i), TOKENS.get(i).add(BigInteger.ONE), "DC2")
                                      )).flatMap(Collection::stream)
                                      .collect(Collectors.toSet());

        int numHosts = INSTANCES.size() / 4;
        for (int i = 0; i < numHosts - 1; i++)
        {
            int hostId = i + 1;
            final List<InstanceMetadata> localInstances = List.of(
            mockInstanceMetadata(1, "dc1-host" + hostId + "-i1", metadata),
            mockInstanceMetadata(2, "dc1-host" + hostId + "-i2", metadata),
            mockInstanceMetadata(3, "dc1-host" + hostId + "-i3", metadata),
            mockInstanceMetadata(4, "dc1-host" + hostId + "-i4", metadata)
            );
            when(instancesMetadata.instances()).thenReturn(localInstances);

            CassandraClientTokenRingProvider cachedLocalTokenRanges = mock(CassandraClientTokenRingProvider.class);
            Set<Host> localHosts = allHosts.stream().filter(host -> host.getAddress()
                                                                        .getHostName()
                                                                        .startsWith("dc1-host" + hostId + "-"))
                                           .collect(Collectors.toSet());
            when(cachedLocalTokenRanges.localInstances()).thenReturn(localHosts);
            Map<Integer, Set<TokenRange>> localRanges = Map.of(
            1, Set.of(new TokenRange(tokenAt(i), tokenAt(i + 1))),
            2, Set.of(new TokenRange(tokenAt(NUM_INSTANCES_PER_HOST + i), tokenAt(NUM_INSTANCES_PER_HOST + i + 1))),
            3, Set.of(new TokenRange(tokenAt(2 * NUM_INSTANCES_PER_HOST + i), tokenAt(2 * NUM_INSTANCES_PER_HOST + i + 1))),
            4, Set.of(new TokenRange(tokenAt(3 * NUM_INSTANCES_PER_HOST + i), tokenAt(3 * NUM_INSTANCES_PER_HOST + i + 1)))
            );
            when(cachedLocalTokenRanges.localTokenRanges(anyString())).thenReturn(localRanges);
            when(cachedLocalTokenRanges.allInstances()).thenReturn(allHosts);

            final InnerDcTokenAdjacentPeerProvider provider = new InnerDcTokenAdjacentPeerProvider(instancesMetadata,
                                                                                                   cachedLocalTokenRanges,
                                                                                                   serviceConfiguration,
                                                                                                   new DnsResolver()
                                                                                                   {
                                                                                                       @Override
                                                                                                       public String resolve(String hostname)
                                                                                                       {
                                                                                                           return hostname;
                                                                                                       }

                                                                                                       @Override
                                                                                                       public String reverseResolve(String address)
                                                                                                       {
                                                                                                           return address;
                                                                                                       }
                                                                                                   });
            final Set<PeerInstance> buddies = provider.get();
            assertEquals(1, buddies.size());
            assertTrue(buddies.stream().findFirst().orElseThrow().hostname().startsWith("dc1-host" + (hostId + 1)));
        }
    }

    @Test
    public void testAdjacentHosts()
    {
        final List<Pair<Host, BigInteger>> allHosts = IntStream.range(0, TOKENS.size())
                                                               .mapToObj(idx -> {
                                                                   BigInteger token = tokenAt(idx);
                                                                   return Pair.of(mockHost(INSTANCES.get(idx), token), token);
                                                               })
                                                               .collect(Collectors.toList());

        final Set<String> adjacentHosts = new HashSet<>(TOKENS.size());
        for (int i = 0; i < TOKENS.size(); i++)
        {
            final String localhost = INSTANCES.get(i);
            final BigInteger token = tokenAt(i);
            test(localhost, token, allHosts, 1, tokenAt(i + 1));
            final Set<Host> adjacent = InnerDcTokenAdjacentPeerProvider.adjacentHosts((host) -> isLocal(localhost,
                                                                                                        host), token, allHosts, 1);
            assertEquals(1, adjacent.size());
            final String adjacentStr = adjacent.stream().findFirst().map(Host::toString).orElseThrow();
            assertFalse(adjacentHosts.contains(adjacentStr));
            adjacentHosts.add(adjacentStr);
        }
        assertEquals(TOKENS.size(), adjacentHosts.size());

        for (int i = 0; i < TOKENS.size(); i++)
        {
            test(INSTANCES.get(i), tokenAt(i), allHosts, 2, tokenAt(i + 1), tokenAt(i + 2));
        }
    }

    @Test
    public void testMinToken()
    {
        final List<TokenRange> tokens = TOKENS.stream().map(start -> new TokenRange(start, start.add(BigInteger.ONE))).collect(Collectors.toList());
        Collections.shuffle(tokens);
        assertEquals(tokenAt(0), InnerDcTokenAdjacentPeerProvider.minToken(tokens.stream()));
    }

    @Test
    public void testRfGreaterThanAvailableHosts()
    {
        List<String> tokens = List.of("-9223372036854775808", "-7686143364045646507", "-6148914691236517206", "-4611686018427387904",
                                      "-3074457345618258603", "-1537228672809129302", "0", "1537228672809129301", "3074457345618258602",
                                      "4611686018427387904", "6148914691236517205", "7686143364045646506");
        List<String> hosts = List.of("local1-i1", "local2-i1", "local3-i1",
                                     "local1-i2", "local2-i2", "local3-i2",
                                     "local1-i3", "local2-i3", "local3-i3",
                                     "local1-i4", "local2-i4", "local3-i4");
        final BigInteger token = new BigInteger(tokens.stream().findFirst().orElseThrow());
        List<Pair<Host, BigInteger>> sortedLocalDcHosts = IntStream.range(0, tokens.size())
                                                                   .mapToObj(i -> {
                                                                       BigInteger t = new BigInteger(tokens.get(i));
                                                                       return Pair.of(mockHost(hosts.get(i), t), t);
                                                                   })
                                                                   .collect(Collectors.toList());
        final int quorum = 5;
        InnerDcTokenAdjacentPeerProvider.adjacentHosts((host) -> host.getAddress().getHostName().startsWith("local1-"),
                                                       token,
                                                       sortedLocalDcHosts,
                                                       quorum)
                                        .stream().map(Host::toString)
                                        .collect(Collectors.toSet());
    }

    private static BigInteger tokenAt(int idx)
    {
        return TOKENS.get(idx % TOKENS.size());
    }

    private static void test(String localhost, BigInteger token, List<Pair<Host, BigInteger>> allHosts, int quorum, BigInteger... expected)
    {
        final Set<String> result = InnerDcTokenAdjacentPeerProvider.adjacentHosts((host) -> isLocal(localhost, host),
                                                                                  token,
                                                                                  allHosts,
                                                                                  quorum)
                                                                    .stream().map(Host::toString)
                                                                    .collect(Collectors.toSet());
        assertFalse(result.contains(token.toString()));
        for (BigInteger bi : expected)
        {
            assertTrue(result.contains(bi.toString()));
        }
        assertEquals(result.size(), expected.length);
    }

    private static boolean isLocal(String localhost, Host host)
    {
        return isLocal(localhost, host.getAddress().getHostName());
    }

    private static boolean isLocal(String localhost, String addr)
    {
        // strip off '-[1-4]' trailing instance number
        return localhost.substring(0, localhost.length() - 3).equals(addr.substring(0, addr.length() - 3));
    }

    public static KeyspaceMetadata mockKeyspace(String name, Map<String, String> replication)
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(name);
        when(keyspaceMetadata.getReplication()).thenReturn(replication);
        return keyspaceMetadata;
    }

    private static InstanceMetadata mockInstanceMetadata(int id, String host, Metadata metadata)
    {
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        when(instanceMetadata.host()).thenReturn(host);
        when(instanceMetadata.id()).thenReturn(id);
        when(instanceMetadata.port()).thenReturn(9043);
        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        when(delegate.metadata()).thenReturn(metadata);
        when(instanceMetadata.delegate()).thenReturn(delegate);
        return instanceMetadata;
    }

    private static Host mockHost(String hostname, BigInteger token)
    {
        return mockHost(hostname, token, "DC1");
    }

    private static Host mockHost(String hostname, BigInteger token, String dc)
    {
        Host host = mock(Host.class);
        InetAddress addr = mock(InetAddress.class);
        when(addr.getHostName()).thenReturn(hostname);
        when(addr.getHostAddress()).thenReturn(hostname);
        when(host.getAddress()).thenReturn(addr);
        when(host.toString()).thenReturn(token.toString());
        when(host.getDatacenter()).thenReturn(dc);
        final Token t = mock(Token.class);
        when(t.getType()).thenReturn(DataType.bigint());
        when(t.getValue()).thenReturn(token.longValue());
        when(host.getTokens()).thenReturn(Set.of(t));
        return host;
    }
}
