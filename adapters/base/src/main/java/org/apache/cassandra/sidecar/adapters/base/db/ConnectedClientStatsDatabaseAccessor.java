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

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.sidecar.adapters.base.db.schema.ConnectedClientsSchema;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.DatabaseAccessor;

/**
 * DataAccessor implementation to read client connection stats from the table represented in {@link ConnectedClientsSchema}
 */
public class ConnectedClientStatsDatabaseAccessor extends DatabaseAccessor<ConnectedClientsSchema>
{
    public ConnectedClientStatsDatabaseAccessor(CQLSessionProvider sessionProvider, ConnectedClientsSchema tableSchema)
    {
        super(tableSchema, sessionProvider);
    }

    /**
     * Query for a summary of the client connection stats
     * @return {@link ConnectedClientStatsSummary} with total connections and counts grouped by user
     */
    public ConnectedClientStatsSummary summary()
    {
        tableSchema.prepareStatements(session());
        BoundStatement statement = tableSchema.connectionsByUser().bind();
        ResultSet resultSet = execute(statement);
        return ConnectedClientStatsSummary.from(resultSet);
    }

    /**
     * Query for all the client connection stats with an entry per connection
     * @return {@link ConnectedClientStats} for each connection
     */
    public Stream<ConnectedClientStats> stats()
    {
        tableSchema.prepareStatements(session());
        BoundStatement statement = tableSchema.stats().bind();
        ResultSet resultSet = execute(statement);
        return StreamSupport.stream(resultSet.spliterator(), false)
                            .map(ConnectedClientStats::from);
    }
}
