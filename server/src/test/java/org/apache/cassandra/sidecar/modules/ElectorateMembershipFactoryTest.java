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

package org.apache.cassandra.sidecar.modules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.ElectorateMembership;
import org.apache.cassandra.sidecar.coordination.MostReplicatedKeyspaceTokenZeroElectorateMembership;
import org.apache.cassandra.sidecar.coordination.SidecarInternalTokenZeroElectorateMembership;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ElectorateMembershipFactory} class
 */
class ElectorateMembershipFactoryTest
{
    private ElectorateMembershipFactory factory;

    @BeforeEach
    void setup()
    {
        factory = new ElectorateMembershipFactory();
    }

    @ParameterizedTest(name = "{index} => strategy=\"{0}\"")
    @ValueSource(strings = { "invalid", "", "a" })
    void testInvalidStrategy(String strategy)
    {
        SidecarConfiguration config = mock(SidecarConfiguration.class, RETURNS_DEEP_STUBS);
        when(config.serviceConfiguration()
                   .coordinationConfiguration()
                   .clusterLeaseClaimConfiguration()
                   .electorateMembershipStrategy()).thenReturn(strategy);

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> factory.create(mock(InstanceMetadataFetcher.class), mock(CQLSessionProvider.class), config))
        .withMessage("Invalid electorate membership strategy value '" + strategy + "'");
    }

    @Test
    void testMostReplicatedKeyspaceTokenZeroElectorateMembership()
    {
        SidecarConfiguration config = mock(SidecarConfiguration.class, RETURNS_DEEP_STUBS);
        when(config.serviceConfiguration()
                   .coordinationConfiguration()
                   .clusterLeaseClaimConfiguration()
                   .electorateMembershipStrategy()).thenReturn(MostReplicatedKeyspaceTokenZeroElectorateMembership.class.getSimpleName());

        ElectorateMembership electorateMembership = factory.create(mock(InstanceMetadataFetcher.class), mock(CQLSessionProvider.class), config);
        assertThat(electorateMembership).isInstanceOf(MostReplicatedKeyspaceTokenZeroElectorateMembership.class);
    }

    @Test
    void testSidecarInternalTokenZeroElectorateMembership()
    {
        SidecarConfiguration config = mock(SidecarConfiguration.class, RETURNS_DEEP_STUBS);
        when(config.serviceConfiguration()
                   .coordinationConfiguration()
                   .clusterLeaseClaimConfiguration()
                   .electorateMembershipStrategy()).thenReturn(SidecarInternalTokenZeroElectorateMembership.class.getSimpleName());

        ElectorateMembership electorateMembership = factory.create(mock(InstanceMetadataFetcher.class), mock(CQLSessionProvider.class), config);
        assertThat(electorateMembership).isInstanceOf(SidecarInternalTokenZeroElectorateMembership.class);
    }
}
