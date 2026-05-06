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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.ReplicationFactorSupplier;
import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcBuilder;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.sidecar.SidecarStatePersister;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
    private TaskExecutorPool taskExecutorPool;
    @Mock
    private CdcDatabaseAccessor cdcDatabaseAccessor;

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
            taskExecutorPool,
            cdcDatabaseAccessor,
            ReplicationFactorSupplier.DEFAULT
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
    void testSingleInstanceSingleRangeCreatesOneConsumer()
    {
        String instanceIp = "127.0.0.1";
        int instanceId = 1;

        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Set<TokenRange> ranges = Collections.singleton(range);
        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(instanceIp, ranges);

        InstanceMetadata instance = mockInstance(instanceId, instanceIp);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        when(cdcConfig.jobId()).thenReturn("test-job");

        // Spy to mock loadOrBuildCdcConsumer
        CdcManager spyManager = spy(cdcManager);
        SidecarCdc mockConsumer = mock(SidecarCdc.class);
        doReturn(mockConsumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

        // Assert
        assertThat(consumers).hasSize(1);
    }

    @Test
    void testSingleInstanceMultipleRangesCreatesMultipleConsumers()
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
        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        when(cdcConfig.jobId()).thenReturn("test-job");

        // Spy to mock loadOrBuildCdcConsumer
        CdcManager spyManager = spy(cdcManager);
        SidecarCdc mockConsumer1 = mock(SidecarCdc.class);
        SidecarCdc mockConsumer2 = mock(SidecarCdc.class);
        doReturn(mockConsumer1, mockConsumer2).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

        // Assert
        assertThat(consumers).hasSize(2);
    }

    @Test
    void testMultipleInstancesMultipleRangesCreatesConsumers()
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

        List<InstanceMetadata> instances = new ArrayList<>();
        instances.add(instance1);
        instances.add(instance2);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.allLocalInstances()).thenReturn(instances);
        when(cdcConfig.jobId()).thenReturn("test-job");

        // Spy to mock loadOrBuildCdcConsumer
        CdcManager spyManager = spy(cdcManager);
        SidecarCdc mockConsumer1 = mock(SidecarCdc.class);
        SidecarCdc mockConsumer2 = mock(SidecarCdc.class);
        doReturn(mockConsumer1, mockConsumer2).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

        // Assert
        assertThat(consumers).hasSize(2);
    }

    @Test
    void testDuplicateRangesDeduplicates()
    {
        String instanceIp = "127.0.0.1";
        int instanceId = 1;

        // Create two identical ranges
        TokenRange range1 = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        TokenRange range2 = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);

        Set<TokenRange> ranges = new HashSet<>();
        ranges.add(range1);
        ranges.add(range2);

        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(instanceIp, ranges);

        InstanceMetadata instance = mockInstance(instanceId, instanceIp);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        when(cdcConfig.jobId()).thenReturn("test-job");

        // Spy to mock loadOrBuildCdcConsumer
        CdcManager spyManager = spy(cdcManager);
        SidecarCdc mockConsumer = mock(SidecarCdc.class);
        doReturn(mockConsumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

        // Assert - Should deduplicate to 1 consumer
        assertThat(consumers).hasSize(1);
    }

    @Test
    void testUnknownInstanceHandlesGracefully()
    {
        String unknownIp = "192.168.1.100";

        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(unknownIp, Collections.singleton(range));

        // No matching instances
        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.emptyList());
        when(cdcConfig.jobId()).thenReturn("test-job");

        // Spy to mock loadOrBuildCdcConsumer - will be called with instanceId = -1
        CdcManager spyManager = spy(cdcManager);
        SidecarCdc mockConsumer = mock(SidecarCdc.class);
        doReturn(mockConsumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

        // Assert - Should still create consumer with instanceId = -1
        assertThat(consumers).hasSize(1);
    }

    @Test
    void testBuildConsumerPassesReplicationFactorSupplierToBuilder() throws Exception
    {
        ReplicationFactorSupplier customSupplier = mock(ReplicationFactorSupplier.class);
        CdcManager managerWithCustomSupplier = new CdcManager(
            eventConsumer, schemaSupplier, cdcConfig, rangeManager, instanceFetcher,
            clusterConfigProvider, sidecarCdcClient,
            cdcStats, taskExecutorPool, cdcDatabaseAccessor, customSupplier);

        try (MockedStatic<SidecarCdc> mockedSidecarCdc = mockStatic(SidecarCdc.class);
             MockedConstruction<SidecarStatePersister> ignoredPersister = mockConstruction(SidecarStatePersister.class))
        {
            SidecarCdcBuilder mockBuilder = mock(SidecarCdcBuilder.class, Answers.RETURNS_SELF);
            mockedSidecarCdc.when(() -> SidecarCdc.builder(
                anyString(), anyInt(), any(), any(), any(), any(), any(), any(), any()
            )).thenReturn(mockBuilder);

            when(cdcConfig.jobId()).thenReturn("test-job");

            managerWithCustomSupplier.loadOrBuildCdcConsumer(
                1, clusterConfigProvider, eventConsumer, schemaSupplier,
                mock(TokenRangeSupplier.class),
                cdcConfig, cdcStats, taskExecutorPool);

            verify(mockBuilder).withReplicationFactorSupplier(customSupplier);
        }
    }

    @Test
    void testResolveToSameAddressTrue()
    {
        String address1 = "127.0.0.1";
        String address2 = "localhost";
        assertThat(CdcManager.resolveToSameAddress(address1, address2)).isTrue();
    }

    @Test
    void testResolveToSameAddressFalse()
    {
        String address1 = "127.0.0.1";
        String address2 = "127.0.0.2";
        assertThat(CdcManager.resolveToSameAddress(address1, address2)).isFalse();
    }

    @Test
    void testStopConsumersWithEmptyConsumersClosesClient() throws Exception
    {
        // No consumers built yet; stopConsumers must still close the client and not throw.
        assertThatCode(() -> cdcManager.stopConsumers()).doesNotThrowAnyException();

        verify(sidecarCdcClient, times(1)).close();
    }

    @Test
    void testStopConsumersInvokesStopOnEachConsumerAndClosesClient() throws Exception
    {
        // Arrange: populate consumers via buildCdcConsumers with two distinct ranges.
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
        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        SidecarCdc consumer1 = mock(SidecarCdc.class);
        SidecarCdc consumer2 = mock(SidecarCdc.class);
        doReturn(consumer1, consumer2).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();
        assertThat(consumers).hasSize(2);

        // Act
        spyManager.stopConsumers();

        // Assert: each consumer was stopped exactly once and the client was closed.
        verify(consumer1, times(1)).stop();
        verify(consumer2, times(1)).stop();
        verify(sidecarCdcClient, times(1)).close();
    }

    @Test
    void testStopConsumersSwallowsExceptionFromClientClose() throws Exception
    {
        // Arrange: build a single consumer, then make sidecarCdcClient.close() throw.
        String instanceIp = "127.0.0.1";
        int instanceId = 1;
        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Map<String, Set<TokenRange>> ownedRanges =
            Collections.singletonMap(instanceIp, Collections.singleton(range));
        InstanceMetadata instance = mockInstance(instanceId, instanceIp);

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        SidecarCdc consumer = mock(SidecarCdc.class);
        doReturn(consumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );
        spyManager.buildCdcConsumers();

        doThrow(new RuntimeException("simulated client close failure"))
            .when(sidecarCdcClient).close();

        // Act + Assert: exception from close() must not propagate.
        assertThatCode(() -> spyManager.stopConsumers()).doesNotThrowAnyException();

        // Consumer must still have been stopped before the failing close() call.
        verify(consumer, times(1)).stop();
        verify(sidecarCdcClient, times(1)).close();
    }

    @Test
    void testStopConsumersDoesNotInteractWithRangeManager() throws Exception
    {
        // stopConsumers() must not consult the RangeManager; it only operates on
        // the previously built consumer list and the cdc client.
        cdcManager.stopConsumers();

        verifyNoInteractions(rangeManager);
        verify(sidecarCdcClient, times(1)).close();
    }

    @Test
    void testStopConsumersIsIdempotent() throws Exception
    {
        // Arrange: build a single consumer.
        String instanceIp = "127.0.0.1";
        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Map<String, Set<TokenRange>> ownedRanges =
            Collections.singletonMap(instanceIp, Collections.singleton(range));
        InstanceMetadata instance = mockInstance(1, instanceIp);
        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        SidecarCdc consumer = mock(SidecarCdc.class);
        doReturn(consumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any()
        );
        spyManager.buildCdcConsumers();

        // Act: invoke twice.
        spyManager.stopConsumers();
        spyManager.stopConsumers();

        // Assert: each call independently stops every consumer and closes the client.
        verify(consumer, times(2)).stop();
        verify(sidecarCdcClient, times(2)).close();
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
}
