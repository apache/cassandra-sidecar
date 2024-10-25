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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStats;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsDatabaseAccessor;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsSummary;
import org.apache.cassandra.sidecar.adapters.base.db.schema.ConnectedClientsSchema;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.jetbrains.annotations.NotNull;

/**
 * Default implementation that pulls methods from the Cassandra Metrics Proxy
 */
public class CassandraMetricsOperations implements MetricsOperations
{
    private final ConnectedClientStatsDatabaseAccessor dbAccessor;

    /**
     * Creates a new instance with the provided {@link CQLSessionProvider}
     */
    public CassandraMetricsOperations(CQLSessionProvider session)
    {
        this.dbAccessor = new ConnectedClientStatsDatabaseAccessor(session, new ConnectedClientsSchema());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectedClientStatsResponse connectedClientStats(boolean summaryOnly)
    {
        if (summaryOnly)
        {
            return connectedClientSummary();
        }
        return connectedClientDetails();
    }

    public ConnectedClientStatsResponse connectedClientDetails()
    {
        List<ClientConnectionEntry> entries = statsToEntries(dbAccessor.stats());
        Map<String, Long> connectionsByUser = entries.stream().collect(Collectors.groupingBy(ClientConnectionEntry::username,
                                                                                             Collectors.counting()));
        long totalConnectedClients = entries.size();
        return new ConnectedClientStatsResponse(entries, totalConnectedClients, connectionsByUser);
    }

    private ConnectedClientStatsResponse connectedClientSummary()
    {
        ConnectedClientStatsSummary summary = dbAccessor.summary();
        return new ConnectedClientStatsResponse(null, summary.totalConnectedClients, summary.connectionsByUser);
    }

    private List<ClientConnectionEntry> statsToEntries(Stream<ConnectedClientStats> stats)
    {
        return stats.map(CassandraMetricsOperations::statToEntry)
                    .collect(Collectors.toList());
    }

    private static @NotNull ClientConnectionEntry statToEntry(ConnectedClientStats stat)
    {
        // Note: We explicitly use constructor params based object creation instead of builder in order to optimize the
        // number of potential objects created for each row of the table queried, specifically since we know this can be large
        return new ClientConnectionEntry(stat.address,
                                         stat.port,
                                         stat.sslEnabled,
                                         stat.sslCipherSuite,
                                         stat.sslProtocol,
                                         stat.protocolVersion,
                                         stat.username,
                                         stat.requestCount,
                                         stat.driverName,
                                         stat.driverVersion,
                                         stat.keyspaceName,
                                         stat.clientOptions,
                                         stat.authenticationMode,
                                         stat.authenticationMetadata);
    }
}
