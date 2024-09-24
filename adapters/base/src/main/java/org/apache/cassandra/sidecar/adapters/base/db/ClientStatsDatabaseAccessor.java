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

package org.apache.cassandra.sidecar.adapters.base.db;

import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.sidecar.adapters.base.db.schema.ClientStatsSchema;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.DatabaseAccessor;

/**
 * DataAccessor implementation to read client connection stats from the table represented in {@link ClientStatsSchema}
 */
public class ClientStatsDatabaseAccessor extends DatabaseAccessor<ClientStatsSchema>
{
    public ClientStatsDatabaseAccessor(CQLSessionProvider sessionProvider, ClientStatsSchema tableSchema)
    {
        super(tableSchema, sessionProvider);
    }

    /**
     * Query for a summary of the client connection stats
     * @return {@link ClientStatsSummary} with total connections and counts grouped by user
     */
    public ClientStatsSummary summary()
    {
        tableSchema.prepareStatements(session());
        BoundStatement statement = tableSchema.connectionCountByUser().bind();
        ResultSet resultSet = execute(statement);
        ClientStatsSummary stats = ClientStatsSummary.from(resultSet);
        return stats;
    }

    /**
     * Query for all the client connection metadata with an entry per connection
     * @return {@link ClientStats} for each connection
     */
    public Set<ClientStats> connections()
    {
        tableSchema.prepareStatements(session());
        BoundStatement statement = tableSchema.listAll().bind();
        ResultSet resultSet = execute(statement);
        Set<ClientStats> stats = resultSet.all().stream().map(ClientStats::from).collect(Collectors.toSet());
        return stats;
    }
}
