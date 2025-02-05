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

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.db.SidecarLeaseDatabaseAccessor;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;

import static org.apache.cassandra.sidecar.coordination.ClusterLeaseClaimTask.MINIMUM_DELAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ClusterLeaseClaimTask} class
 */
class ClusterLeaseClaimTaskTest
{
    @ParameterizedTest(name = "{index} => schemaConfigurationEnabled={0}, featureEnabled={1}")
    @MethodSource(value = { "disabledConfigurationValues" })
    void testSkipWhenConfigurationDisabled(boolean schemaConfigurationEnabled, boolean featureEnabled)
    {
        ServiceConfiguration serviceConfiguration = mockConfiguration(schemaConfigurationEnabled, featureEnabled);
        ElectorateMembership mockElectorateMembership = mock(ElectorateMembership.class);
        ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(mock(Vertx.class), serviceConfiguration, mockElectorateMembership,
                                                               mock(SidecarLeaseDatabaseAccessor.class), new ClusterLease(),
                                                               mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));
        assertThat(task.scheduleDecision()).isEqualTo(ScheduleDecision.SKIP);
        // avoid expensive calls
        verifyNoInteractions(mockElectorateMembership);
    }

    @Test
    void testSkipWhenNonClusterHolder()
    {
        ServiceConfiguration serviceConfiguration = mockConfiguration(true, true);
        ElectorateMembership mockElectorateMembership = mock(ElectorateMembership.class);
        when(mockElectorateMembership.isMember()).thenReturn(false);
        ClusterLease clusterLease = new ClusterLease();
        ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(mock(Vertx.class), serviceConfiguration, mockElectorateMembership,
                                                               mock(SidecarLeaseDatabaseAccessor.class), clusterLease,
                                                               mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));

        assertThat(task.scheduleDecision()).isEqualTo(ScheduleDecision.SKIP);
        assertThat(clusterLease.toScheduleDecision()).as("Skip when not a member of the electorate")
                                                     .isEqualTo(ScheduleDecision.SKIP);
        verify(mockElectorateMembership, times(1)).isMember();
    }

    @Test
    void testShouldNotSkipForMemberOfTheElectorate()
    {
        ServiceConfiguration serviceConfiguration = mockConfiguration(true, true);
        ElectorateMembership mockElectorateMembership = mock(ElectorateMembership.class);
        when(mockElectorateMembership.isMember()).thenReturn(true);
        SidecarLeaseDatabaseAccessor accessor = mock(SidecarLeaseDatabaseAccessor.class);
        when(accessor.isAvailable()).thenReturn(true);
        ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(mock(Vertx.class), serviceConfiguration, mockElectorateMembership,
                                                               accessor, new ClusterLease(),
                                                               mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));

        assertThat(task.scheduleDecision()).isEqualTo(ScheduleDecision.EXECUTE);
        verify(mockElectorateMembership, times(1)).isMember();
    }

    @Test
    void testShouldRescheduleWhenDatabaseAccessorIsUnavailable()
    {
        ServiceConfiguration serviceConfiguration = mockConfiguration(true, true);
        ElectorateMembership mockElectorateMembership = mock(ElectorateMembership.class);
        when(mockElectorateMembership.isMember()).thenReturn(true);
        SidecarLeaseDatabaseAccessor databaseAccessor = mock(SidecarLeaseDatabaseAccessor.class);
        when(databaseAccessor.isAvailable()).thenReturn(false);
        ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(mock(Vertx.class), serviceConfiguration, mockElectorateMembership,
                                                               databaseAccessor, new ClusterLease(),
                                                               mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));

        assertThat(task.scheduleDecision()).isEqualTo(ScheduleDecision.RESCHEDULE);
        verify(mockElectorateMembership, times(1)).isMember();
    }

    @ParameterizedTest(name = "{index} => configuredInitialDelay {0} millis")
    @ValueSource(longs = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 })
    void testInitialDelayFromConfiguration(long configuredDelayMillis)
    {
        ServiceConfiguration mockServiceConfiguration = mock(ServiceConfiguration.class, RETURNS_DEEP_STUBS);
        when(mockServiceConfiguration.coordinationConfiguration().clusterLeaseClaimConfiguration().initialDelay().quantity())
        .thenReturn(configuredDelayMillis);
        when(mockServiceConfiguration.coordinationConfiguration().clusterLeaseClaimConfiguration().initialDelay().unit())
        .thenReturn(TimeUnit.MILLISECONDS);
        when(mockServiceConfiguration.coordinationConfiguration().clusterLeaseClaimConfiguration().initialDelay().to(TimeUnit.MILLISECONDS))
        .thenCallRealMethod();
        ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(mock(Vertx.class), mockServiceConfiguration, mock(ElectorateMembership.class),
                                                               mock(SidecarLeaseDatabaseAccessor.class), new ClusterLease(),
                                                               mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));

        assertThat(task.initialDelay().to(TimeUnit.MILLISECONDS)).isEqualTo(configuredDelayMillis);
    }

    @ParameterizedTest(name = "{index} => configuredDelayMillis {0} millis")
    @ValueSource(longs = { 30_000, 40_000, 50_000, 100_000, 1_000_000, 10_000_000, 20_000_000, Long.MAX_VALUE - 1 })
    void testDelayFromConfiguration(long configuredDelayMillis)
    {
        ServiceConfiguration mockServiceConfiguration = mock(ServiceConfiguration.class, RETURNS_DEEP_STUBS);
        MillisecondBoundConfiguration value = new MillisecondBoundConfiguration(configuredDelayMillis, TimeUnit.MILLISECONDS);
        when(mockServiceConfiguration.coordinationConfiguration()
                                     .clusterLeaseClaimConfiguration()
                                     .executeInterval()).thenReturn(value);
        ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(mock(Vertx.class), mockServiceConfiguration, mock(ElectorateMembership.class),
                                                               mock(SidecarLeaseDatabaseAccessor.class), new ClusterLease(),
                                                               mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));

        assertThat(task.delay().to(TimeUnit.MILLISECONDS)).isEqualTo(configuredDelayMillis);
    }

    @Test
    void testCannotConfigureDelayLessThanMinimum()
    {
        ServiceConfiguration mockServiceConfiguration = mock(ServiceConfiguration.class, RETURNS_DEEP_STUBS);
        MillisecondBoundConfiguration lessThanMinimum = MillisecondBoundConfiguration.parse("29s");
        when(mockServiceConfiguration.coordinationConfiguration().clusterLeaseClaimConfiguration().executeInterval()).thenReturn(lessThanMinimum);
        ClusterLeaseClaimTask task = new ClusterLeaseClaimTask(mock(Vertx.class), mockServiceConfiguration, mock(ElectorateMembership.class),
                                                               mock(SidecarLeaseDatabaseAccessor.class), new ClusterLease(),
                                                               mock(SidecarMetrics.class, RETURNS_DEEP_STUBS));
        assertThat(task.delay()).as("The minimum is guaranteed").isEqualTo(MINIMUM_DELAY);
    }

    private ServiceConfiguration mockConfiguration(boolean schemaConfigurationEnabled, boolean featureEnabled)
    {
        ServiceConfiguration mockServiceConfiguration = mock(ServiceConfiguration.class, RETURNS_DEEP_STUBS);
        when(mockServiceConfiguration.schemaKeyspaceConfiguration().isEnabled()).thenReturn(schemaConfigurationEnabled);
        when(mockServiceConfiguration.coordinationConfiguration().clusterLeaseClaimConfiguration().enabled()).thenReturn(featureEnabled);

        return mockServiceConfiguration;
    }

    static Stream<Arguments> disabledConfigurationValues()
    {
        return Stream.of(Arguments.of(false, false),
                         Arguments.of(false, true),
                         Arguments.of(true, false));
    }
}
