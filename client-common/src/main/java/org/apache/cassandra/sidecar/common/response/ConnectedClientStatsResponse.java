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

package org.apache.cassandra.sidecar.common.response;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;

/**
 * Class response for the ConnectedClientStats API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConnectedClientStatsResponse
{

    @JsonProperty("clientConnections")
    public List<ClientConnectionEntry> clientConnections()
    {
        return clientConnections;
    }

    @JsonProperty("totalConnectedClients")
    public long totalConnectedClients()
    {
        return totalConnectedClients;
    }

    @JsonProperty("connectionsByUser")
    public Map<String, Long> connectionsByUser()
    {
        return connectionsByUser;
    }

    private final List<ClientConnectionEntry> clientConnections;
    private final long totalConnectedClients;
    private final Map<String, Long> connectionsByUser;


    /**
     * Constructs a new {@link ConnectedClientStatsResponse}.
     *
     * @param clientConnections list of client connection stats
     * @param totalConnectedClients total count of the connected clients
     * @param connectionsByUser mapping of user to the no. connections
     */
    @JsonCreator
    public ConnectedClientStatsResponse(@JsonProperty("clientConnections") List<ClientConnectionEntry> clientConnections,
                                        @JsonProperty("totalConnectedClients") long totalConnectedClients,
                                        @JsonProperty("connectionsByUser") Map<String, Long> connectionsByUser)
    {
        this.clientConnections = clientConnections;
        this.totalConnectedClients = totalConnectedClients;
        this.connectionsByUser = connectionsByUser;
    }
}
