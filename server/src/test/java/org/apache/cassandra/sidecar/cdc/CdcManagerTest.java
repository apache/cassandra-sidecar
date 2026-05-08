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
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.ReplicationFactorSupplier;
import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcBuilder;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.sidecar.SidecarStatePersister;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
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
    private CdcSidecarInstancesProvider sidecarInstancesProvider;
    @Mock
    private SecretsProvider secretsProvider;
    @Mock
    private SidecarCdcClient.ClientConfig clientConfig;
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
            sidecarInstancesProvider,
            secretsProvider,
            clientConfig,
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
        SidecarCdc mockConsumer = mock(SidecarCdc.class);
        doReturn(mockConsumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

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
        SidecarCdc mockConsumer1 = mock(SidecarCdc.class);
        SidecarCdc mockConsumer2 = mock(SidecarCdc.class);
        doReturn(mockConsumer1, mockConsumer2).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

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
        SidecarCdc mockConsumer1 = mock(SidecarCdc.class);
        SidecarCdc mockConsumer2 = mock(SidecarCdc.class);
        doReturn(mockConsumer1, mockConsumer2).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

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
        SidecarCdc mockConsumer = mock(SidecarCdc.class);
        doReturn(mockConsumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(1);
    }

    @Test
    void testUnknownInstanceHandlesGracefully() throws IOException
    {
        String unknownIp = "192.168.1.100";

        TokenRange range = mockTokenRange(BigInteger.ZERO, BigInteger.TEN);
        Map<String, Set<TokenRange>> ownedRanges = Collections.singletonMap(unknownIp, Collections.singleton(range));

        when(rangeManager.ownedTokenRanges()).thenReturn(ownedRanges);
        when(instanceFetcher.instance(unknownIp))
            .thenThrow(new NoSuchCassandraInstanceException("Instance not found: " + unknownIp));
        when(cdcConfig.jobId()).thenReturn("test-job");

        CdcManager spyManager = spy(cdcManager);
        SidecarCdc mockConsumer = mock(SidecarCdc.class);
        doReturn(mockConsumer).when(spyManager).loadOrBuildCdcConsumer(
            anyInt(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()
        );

        List<SidecarCdc> consumers = spyManager.buildCdcConsumers();

        assertThat(consumers).hasSize(1);
    }

    @Test
    void testBuildConsumerPassesReplicationFactorSupplierToBuilder() throws Exception
    {
        ReplicationFactorSupplier customSupplier = mock(ReplicationFactorSupplier.class);
        CdcManager managerWithCustomSupplier = new CdcManager(
            eventConsumer, schemaSupplier, cdcConfig, rangeManager, instanceFetcher,
            clusterConfigProvider, sidecarInstancesProvider, secretsProvider, clientConfig,
            cdcStats, taskExecutorPool, cdcDatabaseAccessor, customSupplier);

        try (MockedStatic<SidecarCdc> mockedSidecarCdc = mockStatic(SidecarCdc.class);
             MockedConstruction<SidecarStatePersister> ignoredPersister = mockConstruction(SidecarStatePersister.class))
        {
            SidecarCdcBuilder mockBuilder = mock(SidecarCdcBuilder.class, Answers.RETURNS_SELF);
            mockedSidecarCdc.when(() -> SidecarCdc.builder(
                anyString(), anyInt(), any(), any(), any(), any(), any(), any(), any(), any(), any()
            )).thenReturn(mockBuilder);

            when(cdcConfig.jobId()).thenReturn("test-job");

            managerWithCustomSupplier.loadOrBuildCdcConsumer(
                1, clusterConfigProvider, eventConsumer, schemaSupplier,
                mock(TokenRangeSupplier.class),
                sidecarInstancesProvider, secretsProvider, clientConfig,
                cdcConfig, cdcStats, taskExecutorPool);

            verify(mockBuilder).withReplicationFactorSupplier(customSupplier);
        }
    }

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
