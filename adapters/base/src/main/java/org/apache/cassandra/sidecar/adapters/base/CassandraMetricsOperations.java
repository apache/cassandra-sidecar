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

package org.apache.cassandra.sidecar.adapters.base;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStats;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsDatabaseAccessor;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsSummary;
import org.apache.cassandra.sidecar.adapters.base.db.schema.ClientStatsSchema;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;

/**
 * Default implementation that pulls methods from the Cassandra Metrics Proxy
 */
public class CassandraMetricsOperations implements MetricsOperations
{
    private ConnectedClientStatsDatabaseAccessor dbAccessor;

    /**
     * Creates a new instance with the provided {@link JmxClient}
     */
    public CassandraMetricsOperations(CQLSessionProvider session)
    {
        this.dbAccessor = new ConnectedClientStatsDatabaseAccessor(session, new ClientStatsSchema());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectedClientStatsResponse connectedClientStats(boolean isListConnections)
    {
        ConnectedClientStatsResponse.Builder response = ConnectedClientStatsResponse.builder();

        if (isListConnections)
        {
            Set<ClientConnectionEntry> entries = transform(dbAccessor.stats());
            Map<String, Long> connectionsByUser = entries.stream()
                                                         .collect(Collectors.groupingBy(ClientConnectionEntry::user,
                                                                                        Collectors.counting()));
            int totalConnectedClients = connectionsByUser.values().stream().mapToInt(Math::toIntExact).sum();
            response.clientConnections(entries);
            response.connectionsByUser(connectionsByUser);
            response.totalConnectedClients(totalConnectedClients);
        }
        else
        {
            ConnectedClientStatsSummary summary = dbAccessor.summary();
            response.connectionsByUser(summary.connectionsByUser);
            response.totalConnectedClients(summary.totalConnectedClients);
        }
        return response.build();
    }

    private Set<ClientConnectionEntry> transform(Set<ConnectedClientStats> stats)
    {

        return stats.stream().map(stat -> {
            ClientConnectionEntry.Builder b = ClientConnectionEntry.builder();
            b.address(stat.address);
            b.port(stat.port);
            b.user(stat.username);
            b.version(stat.protocolVersion);
            b.driverName(stat.driverName);
            b.driverVersion(stat.driverVersion);
            b.ssl(stat.sslEnabled);
            b.protocol(stat.sslProtocol);
            b.requests(stat.requestCount);
            return b.build();
        }).collect(Collectors.toSet());
    }
}
