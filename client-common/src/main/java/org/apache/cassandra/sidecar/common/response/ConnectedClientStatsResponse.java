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

import com.google.common.annotations.VisibleForTesting;

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
     * Constructs a new {@link ConnectedClientStatsResponse} from the configured {@link Builder}.
     *
     * @param builder the builder used to create this object
     */
    @VisibleForTesting
    protected ConnectedClientStatsResponse(Builder builder)
    {
        clientConnections = builder.clientConnections;
        totalConnectedClients = builder.totalConnectedClients;
        connectionsByUser = builder.connectionsByUser;
    }

    public ConnectedClientStatsResponse()
    {
        this(builder());
    }

    /**
     * @return a new {@code ClientStatsResponse} builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code ClientStatsResponse} builder static inner class.
     */
    public static final class Builder
    {
        private List<ClientConnectionEntry> clientConnections;
        private long totalConnectedClients;

        private Map<String, Long> connectionsByUser;

        /**
         * Sets the client connection metadata and returns a reference to this Builder enabling method chaining.
         *
         * @param clientConnections the list of {@code clientConnections} to set
         * @return a reference to this Builder
         */
        public Builder clientConnections(List<ClientConnectionEntry> clientConnections)
        {
            this.clientConnections = clientConnections;
            return this;
        }

        /**
         * Sets the {@code totalConnectedClients} and returns a reference to this Builder enabling method chaining.
         *
         * @param totalConnectedClients the {@code totalConnectedClients} to set
         * @return a reference to this Builder
         */
        public Builder totalConnectedClients(long totalConnectedClients)
        {
            this.totalConnectedClients = totalConnectedClients;
            return this;
        }

        /**
         * Sets the {@code connectionsByUser} and returns a reference to this Builder enabling method chaining.
         *
         * @param connectionsByUser the {@code connectionsByUser} to set
         * @return a reference to this Builder
         */
        public Builder connectionsByUser(Map<String, Long> connectionsByUser)
        {
            this.connectionsByUser = connectionsByUser;
            return this;
        }

        private Builder()
        {
        }

        public ConnectedClientStatsResponse build()
        {
            return new ConnectedClientStatsResponse(this);
        }
    }
}
