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

package org.apache.cassandra.sidecar.cdc;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.sidecar.SidecarCdcStats;
import org.apache.cassandra.cdc.sidecar.SidecarStatePersister;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CdcManager
 */
public class CdcManagerTest
{
    @Mock
    private EventConsumer eventConsumer;
    @Mock
    private SchemaSupplier schemaSupplier;
    @Mock
    private CdcConfig cdcConfig;
    @Mock
    private RangeManager rangeManager;
    @Mock
    private InstanceMetadataFetcher instanceFetcher;
    @Mock
    private ClusterConfigProvider clusterConfigProvider;
    @Mock
    private SidecarCdcClient sidecarCdcClient;
    @Mock
    private ICdcStats cdcStats;
    @Mock
    private SidecarCdcStats sidecarCdcStats;
    @Mock
    private TaskExecutorPool taskExecutorPool;
    @Mock
    private CdcDatabaseAccessor cdcDatabaseAccessor;
    @Mock
    private CdcOptions cdcOptions;

    private CdcManager cdcManager;

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.openMocks(this);

        cdcManager = new CdcManager(
            eventConsumer,
            schemaSupplier,
            cdcConfig,
            rangeManager,
            instanceFetcher,
            clusterConfigProvider,
            sidecarCdcClient,
            cdcStats,
            sidecarCdcStats,
            taskExecutorPool,
            cdcDatabaseAccessor,
            cdcOptions
        );
    }

    @Test
    void testNullOwnedRangesThrowsException()
    {
        when(rangeManager.ownedTokenRanges()).thenReturn(null);

        assertThatThrownBy(() -> cdcManager.buildCdcConsumers())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No owned token ranges");
    }

    @Test
    void testEmptyOwnedRangesThrowsException()
    {
        when(rangeManager.ownedTokenRanges()).thenReturn(Collections.emptyMap());

        assertThatThrownBy(() -> cdcManager.buildCdcConsumers())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No owned token ranges");
    }

    @Test
    void testSingleInstanceSingleRangeCreatesOneConsumer() throws IOException
    {
        String instanceIp = "127.0.0.1";
        int instanceId = 1;

        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Set<TokenRange> ranges = Collections.singleton(range);
        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(instanceIp, ranges);

        InstanceMetadata instance = mockInstance(instanceId, instanceIp);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.instance(instanceIp)).thenReturn(instance);
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        CdcConsumerEntry mockEntry = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        doReturn(mockEntry).when(spyManager).buildConsumer(
            any(), anyInt(), any(), any(), any(), any(), any(), any()
        );

        List<CdcConsumerEntry> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(1);
    }

    @Test
    void testSingleInstanceMultipleRangesCreatesMultipleConsumers() throws IOException
    {
        String instanceIp = "127.0.0.1";
        int instanceId = 1;

        TokenRange range1 = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        TokenRange range2 = mockTokenRange(BigInteger.TEN, new BigInteger("20"));
        Set<TokenRange> ranges = new HashSet<>();
        ranges.add(range1);
        ranges.add(range2);

        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(instanceIp, ranges);

        InstanceMetadata instance = mockInstance(instanceId, instanceIp);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.instance(instanceIp)).thenReturn(instance);
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        CdcConsumerEntry mockEntry1 = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        CdcConsumerEntry mockEntry2 = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        doReturn(mockEntry1, mockEntry2).when(spyManager).buildConsumer(
            any(), anyInt(), any(), any(), any(), any(), any(), any()
        );

        List<CdcConsumerEntry> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(2);
    }

    @Test
    void testMultipleInstancesMultipleRangesCreatesConsumers() throws IOException
    {
        String instance1Ip = "127.0.0.1";
        String instance2Ip = "127.0.0.2";
        int instance1Id = 1;
        int instance2Id = 2;

        TokenRange range1 = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        TokenRange range2 = mockTokenRange(BigInteger.TEN, new BigInteger("20"));

        Map<String, Set<TokenRange>> ownedRanges = new HashMap<>();
        ownedRanges.put(instance1Ip, Collections.singleton(range1));
        ownedRanges.put(instance2Ip, Collections.singleton(range2));

        InstanceMetadata instance1 = mockInstance(instance1Id, instance1Ip);
        InstanceMetadata instance2 = mockInstance(instance2Id, instance2Ip);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.instance(instance1Ip)).thenReturn(instance1);
        when(instanceFetcher.instance(instance2Ip)).thenReturn(instance2);
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        CdcConsumerEntry mockEntry1 = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        CdcConsumerEntry mockEntry2 = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        doReturn(mockEntry1, mockEntry2).when(spyManager).buildConsumer(
            any(), anyInt(), any(), any(), any(), any(), any(), any()
        );

        List<CdcConsumerEntry> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(2);
    }

    @Test
    void testDuplicateRangesDeduplicates() throws IOException
    {
        String instanceIp = "127.0.0.1";
        int instanceId = 1;

        TokenRange range1 = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        TokenRange range2 = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);

        Set<TokenRange> ranges = new HashSet<>();
        ranges.add(range1);
        ranges.add(range2);

        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(instanceIp, ranges);

        InstanceMetadata instance = mockInstance(instanceId, instanceIp);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.instance(instanceIp)).thenReturn(instance);
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        CdcConsumerEntry mockEntry = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        doReturn(mockEntry).when(spyManager).buildConsumer(
            any(), anyInt(), any(), any(), any(), any(), any(), any()
        );

        List<CdcConsumerEntry> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(1);
    }

    @Test
    void testUnknownInstanceHandlesGracefully() throws IOException
    {
        String unknownIp = "192.168.1.100";

        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(unknownIp, Collections.singleton(range));

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.instance(unknownIp)).thenThrow(new NoSuchCassandraInstanceException("Instance not found: " + unknownIp));
        when(cdcConfig.jobId()).thenReturn("test-job");

        // Spy to mock buildConsumer - will be called with instanceId = -1
        CdcManager spyManager = spy(cdcManager);
        CdcConsumerEntry mockEntry = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        doReturn(mockEntry).when(spyManager).buildConsumer(
            any(), anyInt(), any(), any(), any(), any(), any(), any()
        );

        List<CdcConsumerEntry> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(1);
    }

    @Test
    void testResolveToSameAddressTrue()
    {
        assertThat(resolveToSameAddress("127.0.0.1", "localhost")).isTrue();
    }

    @Test
    void testResolveToSameAddressFalse()
    {
        assertThat(resolveToSameAddress("127.0.0.1", "127.0.0.2")).isFalse();
    }

    /**
     * Verifies that the correct instanceId is propagated into {@code loadOrBuildCdcConsumer}
     * during the full {@code buildCdcConsumers()} flow when {@code ipAddress()} is null.
     * Complements {@code testGetInstanceIdReturnsCorrectIdWhenIpAddressIsNull}, which tests
     * {@code getInstanceId} in isolation; this test confirms the fix is effective end-to-end.
     */
    @Test
    void testGetInstanceIdResolvesCorrectlyWhenIpAddressIsNull() throws IOException
    {
        String instanceIp = "172.19.0.5";
        int instanceId = 1000;

        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(instanceIp, Collections.singleton(range));

        InstanceMetadata instance = mock(InstanceMetadata.class, RETURNS_DEEP_STUBS);
        when(instance.id()).thenReturn(instanceId);
        when(instance.ipAddress()).thenReturn(null);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.instance(instanceIp)).thenReturn(instance);
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        CdcConsumerEntry mockEntry = new CdcConsumerEntry(mock(SidecarCdc.class), mock(SidecarStatePersister.class), mock(SidecarCdcStats.class));
        doReturn(mockEntry).when(spyManager).buildConsumer(
                any(), anyInt(), any(), any(), any(), any(), any(), any()
        );

        List<CdcConsumerEntry> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(1);
        verify(spyManager).buildConsumer(
            any(), eq(instanceId), any(), any(), any(), any(), any(), any()
        );
    }

    /**
     * Unit test for the CASSSIDECAR-417 bug fix: {@code getInstanceId} must return the correct id
     * even when {@code ipAddress()} is null (not yet refreshed). The old code passed {@code null}
     * to {@code resolveToSameAddress}, which resolved to {@code 127.0.0.1} and returned {@code -1}.
     * The fix resolves the instance via {@code instanceFetcher.instance(ip)} instead.
     */
    @Test
    void testGetInstanceIdReturnsCorrectIdWhenIpAddressIsNull()
    {
        String instanceIp = "172.19.0.5";
        int instanceId = 1;

        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(instanceId);
        when(instance.ipAddress()).thenReturn(null); // not yet refreshed — the key precondition

        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        when(instanceFetcher.instance(instanceIp)).thenReturn(instance);

        assertThat(cdcManager.getInstanceId(instanceIp)).isEqualTo(instanceId);
    }

    /**
     * Verifies that getInstanceId returns -1 when the IP is not known to any local instance.
     * Both old and new code produce -1 here, but via different mechanisms.
     */
    @Test
    void testGetInstanceIdReturnsMinusOneWhenInstanceNotFound()
    {
        String unknownIp = "192.168.1.100";

        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.emptyList());
        when(instanceFetcher.instance(unknownIp))
            .thenThrow(new NoSuchCassandraInstanceException("Instance not found: " + unknownIp));

        assertThat(cdcManager.getInstanceId(unknownIp)).isEqualTo(-1);
    }

    /**
     * Regression guard: {@code SidecarStatePersister} was previously built with the
     * cassandra-analytics-cdc-sidecar {@code SidecarCdcOptions.DEFAULT}, which pinned
     * {@code persistDelay()} to its hardcoded 1000ms interface default regardless of what
     * operators configured in the "configs" table. {@link CdcManager.ConfigBackedPersisterOptions}
     * fixes this by delegating {@code persistDelay()} straight to {@link CdcConfig}; this test
     * uses a value that differs from both the interface default (1000ms) and the
     * {@code CdcConfigImpl} default (also 1000ms) so a pass proves real delegation.
     */
    @Test
    void configBackedPersisterOptionsDelegatesPersistDelayToCdcConfig()
    {
        when(cdcConfig.persistDelay()).thenReturn(new MillisecondBoundConfiguration(2500, TimeUnit.MILLISECONDS));

        CdcManager.ConfigBackedPersisterOptions persisterOptions = new CdcManager.ConfigBackedPersisterOptions(cdcConfig);

        assertThat(persisterOptions.persistDelay()).isEqualTo(Duration.ofMillis(2500));
    }

    // Helper methods

    private TokenRange mockTokenRange(BigInteger start, BigInteger end)
    {
        TokenRange range = mock(TokenRange.class, RETURNS_DEEP_STUBS);
        when(range.startAsBigInt()).thenReturn(start);
        when(range.endAsBigInt()).thenReturn(end);
        return range;
    }

    private InstanceMetadata mockInstance(int id, String ipAddress)
    {
        InstanceMetadata instance = mock(InstanceMetadata.class, RETURNS_DEEP_STUBS);
        when(instance.id()).thenReturn(id);
        when(instance.ipAddress()).thenReturn(ipAddress);
        return instance;
    }

    private static boolean resolveToSameAddress(String address1, String address2)
    {
        try
        {
            InetAddress addr1 = InetAddress.getByName(address1);
            InetAddress addr2 = InetAddress.getByName(address2);
            return addr1.equals(addr2);
        }
        catch (UnknownHostException e)
        {
            return address1.equals(address2);
        }
    }
}
