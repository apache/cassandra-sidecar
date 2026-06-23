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

package org.apache.cassandra.sidecar.cluster.locator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;

import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CachedLocalTokenRanges}
 */
public class CachedLocalTokenRangesTest
{
    private Metadata metadata;
    private CachedLocalTokenRanges cachedLocalTokenRanges;

    @BeforeEach
    void setup()
    {
        metadata = mock(Metadata.class);
        InstancesMetadata instancesMetadata = mock(InstancesMetadata.class);
        DnsResolver dnsResolver = mock(DnsResolver.class);

        InstanceMetadata instance = mock(InstanceMetadata.class, RETURNS_DEEP_STUBS);
        when(instance.delegate().metadata()).thenReturn(metadata);
        when(instancesMetadata.instances()).thenReturn(List.of(instance));

        cachedLocalTokenRanges = new CachedLocalTokenRanges(instancesMetadata, dnsResolver);
    }

    @Test
    void testLocalTokenRanges_succeedsForMixedCaseKeyspace()
    {
        // Keyspaces created as quoted CQL identifiers (e.g. CREATE KEYSPACE "MyKeyspace")
        // must be looked up with Metadata.quoteIfNecessary() to trigger a case-sensitive
        // lookup in the DataStax Java Driver. Without it, the driver normalizes the name
        // to lowercase and fails to find the keyspace.
        String keyspace = "MyKeyspace";
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(keyspace);
        when(metadata.getKeyspace(Metadata.quoteIfNecessary(keyspace))).thenReturn(keyspaceMetadata);
        when(metadata.getAllHosts()).thenReturn(Collections.emptySet());
        when(metadata.getKeyspaces()).thenReturn(Collections.emptyList());

        Map<Integer, Set<TokenRange>> result = cachedLocalTokenRanges.localTokenRanges(keyspace);

        assertNotNull(result);
        // Verify the driver was called with the quoted form, not the raw mixed-case string
        verify(metadata).getKeyspace(Metadata.quoteIfNecessary(keyspace));
    }

    @Test
    void testLocalTokenRanges_succeedsForUnquotedMixedCaseKeyspace()
    {
        // Regression: MyKeyspace created without CQL quotes (Cassandra internal name: mykeyspace).
        // Sidecar stores MyKeyspace. The raw lookup simulates the driver's case-folding and returns
        // the keyspace; the quoteIfNecessary fallback must not be reached, because quoteIfNecessary
        // would look for case-sensitive MyKeyspace (not mykeyspace) and return null.
        String keyspace = "MyKeyspace";
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn("mykeyspace");
        when(metadata.getKeyspace(keyspace)).thenReturn(keyspaceMetadata);
        when(metadata.getAllHosts()).thenReturn(Collections.emptySet());
        when(metadata.getKeyspaces()).thenReturn(Collections.emptyList());

        Map<Integer, Set<TokenRange>> result = cachedLocalTokenRanges.localTokenRanges(keyspace);

        assertNotNull(result);
        verify(metadata, never()).getKeyspace(Metadata.quoteIfNecessary(keyspace));
    }

    @Test
    void testLocalTokenRanges_throwsWhenKeyspaceDoesNotExist()
    {
        when(metadata.getKeyspace(Metadata.quoteIfNecessary("nonexistent"))).thenReturn(null);
        when(metadata.getAllHosts()).thenReturn(Collections.emptySet());

        assertThatThrownBy(() -> cachedLocalTokenRanges.localTokenRanges("nonexistent"))
            .isInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("Keyspace does not exist. keyspace: nonexistent");
    }
}
