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

import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.ElectorateMembership;
import org.apache.cassandra.sidecar.coordination.MostReplicatedKeyspaceTokenZeroElectorateMembership;
import org.apache.cassandra.sidecar.coordination.SidecarInternalTokenZeroElectorateMembership;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Factory class to initialize the {@link org.apache.cassandra.sidecar.coordination.ElectorateMembership} instance
 * based on the configured strategy
 */
public class ElectorateMembershipFactory
{
    /**
     * Creates the {@link ElectorateMembership} based on the strategy configuration
     *
     * @param fetcher         the interface to retrieve instance metadata
     * @param sessionProvider the provider for the CQL session
     * @param config          the configuration for running Sidecar
     * @return the created {@link ElectorateMembership} instance
     * @throws ConfigurationException when an invalid strategy is used
     */
    public ElectorateMembership create(InstanceMetadataFetcher fetcher,
                                       CQLSessionProvider sessionProvider,
                                       SidecarConfiguration config)
    {
        String strategy = config.serviceConfiguration()
                                .coordinationConfiguration()
                                .clusterLeaseClaimConfiguration()
                                .electorateMembershipStrategy();
        switch (strategy)
        {
            case "MostReplicatedKeyspaceTokenZeroElectorateMembership":
                return new MostReplicatedKeyspaceTokenZeroElectorateMembership(fetcher, sessionProvider, config);
            case "SidecarInternalTokenZeroElectorateMembership":
                return new SidecarInternalTokenZeroElectorateMembership(fetcher, config);
            default:
                throw new ConfigurationException("Invalid electorate membership strategy value '" + strategy + "'");
        }
    }
}
