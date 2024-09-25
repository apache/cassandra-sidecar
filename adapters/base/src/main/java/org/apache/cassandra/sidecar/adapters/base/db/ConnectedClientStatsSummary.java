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

import java.util.Map;
import java.util.stream.Collectors;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.db.DataObjectMappingException;
import org.jetbrains.annotations.NotNull;

/**
 * Representation of a summary of client connections stats
 */
public class ConnectedClientStatsSummary
{
    public final int totalConnectedClients;
    public final Map<String, Long> connectionsByUser;

    public static ConnectedClientStatsSummary.Builder builder()
    {
        return new ConnectedClientStatsSummary.Builder();
    }

    public static ConnectedClientStatsSummary from(@NotNull ResultSet resultSet) throws DataObjectMappingException
    {

        Map<String, Long> resultMap = resultSet.all().stream()
                                               .collect(Collectors.toMap(r -> r.getString("username"),
                                                                         r -> r.getLong("connection_count")));
        int totalConnections = resultMap.values().stream().mapToInt(Math::toIntExact).sum();

        ConnectedClientStatsSummary.Builder builder = new ConnectedClientStatsSummary.Builder();
        builder.connectionsByUser(resultMap);
        builder.totalConnectedClients(totalConnections);

        return builder.build();
    }

    private ConnectedClientStatsSummary(ConnectedClientStatsSummary.Builder builder)
    {
        this.connectionsByUser = builder.connectionsByUser;
        this.totalConnectedClients = builder.totalConnectedClients;
    }

    /**
     * Builder for {@link ConnectedClientStatsSummary}
     */
    public static class Builder implements DataObjectBuilder<ConnectedClientStatsSummary.Builder, ConnectedClientStatsSummary>
    {
        private int totalConnectedClients;
        private Map<String, Long> connectionsByUser;

        private Builder()
        {
        }

        private Builder(ConnectedClientStatsSummary summary)
        {
            this.connectionsByUser = summary.connectionsByUser;
            this.totalConnectedClients = summary.totalConnectedClients;
        }

        public ConnectedClientStatsSummary.Builder totalConnectedClients(int count)
        {
            return update(b -> b.totalConnectedClients = count);
        }

        public ConnectedClientStatsSummary.Builder connectionsByUser(Map<String, Long> connections)
        {
            return update(b -> b.connectionsByUser = connections);
        }

        public ConnectedClientStatsSummary.Builder self()
        {
            return this;
        }

        public ConnectedClientStatsSummary build()
        {
            return new ConnectedClientStatsSummary(this);
        }
    }
}

