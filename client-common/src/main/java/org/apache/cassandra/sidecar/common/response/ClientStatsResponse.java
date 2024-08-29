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

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class response for the ClientStats API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClientStatsResponse
{

    /**
     * Represents the different stat fields returned for client connection stats
     */
    public enum Connections
    {
        address, user, version, clientOptions, driverName, driverVersion, requests, keyspace, ssl, cipher, protocol, authenticationMode, authenticationMetadata
    }

    /**
     * Represents the different stat fields returned for client protocol-version stats
     */
    public enum ProtocolVersion
    {
        protocolVersion, inetAddress, lastSeenTime
    }

    // clientsByProtocolVersion
    private final String remoteInetAddress;
    private final String protocolVersion;
    private final String lastSeenTime;

    @JsonProperty("address")
    public String address()
    {
        return address;
    }

    @JsonProperty("ssl")
    public Boolean ssl()
    {
        return ssl;
    }

    @JsonProperty("cipher")
    public String cipher()
    {
        return cipher;
    }

    @JsonProperty("protocol")
    public String protocol()
    {
        return protocol;
    }

    @JsonProperty("version")
    public String version()
    {
        return version;
    }

    @JsonProperty("user")
    public String user()
    {
        return user;
    }

    @JsonProperty("keyspace")
    public String keyspace()
    {
        return keyspace;
    }

    @JsonProperty("requests")
    public String requests()
    {
        return requests;
    }

    @JsonProperty("driverName")
    public String driverName()
    {
        return driverName;
    }

    @JsonProperty("driverVersion")
    public String driverVersion()
    {
        return driverVersion;
    }

    @JsonProperty("clientOptions")
    public Map<String, String> clientOptions()
    {
        return clientOptions;
    }

    @JsonProperty("authenticationMode")
    public String authenticationMode()
    {
        return authenticationMode;
    }

    @JsonProperty("authenticationMetadata")
    public String authenticationMetadata()
    {
        return authenticationMetadata;
    }

    @JsonProperty("totalConnectedClients")
    public int totalConnectedClients()
    {
        return totalConnectedClients;
    }

    @JsonProperty("connectionsByUser")
    public Map<String, Integer> connectionsByUser()
    {
        return connectionsByUser;
    }

    @JsonProperty("remoteInetAddress")
    public String remoteInetAddress()
    {
        return remoteInetAddress;
    }

    @JsonProperty("protocolVersion")
    public String protocolVersion()
    {
        return protocolVersion;
    }

    @JsonProperty("lastSeenTime")
    public String lastSeenTime()
    {
        return lastSeenTime;
    }

    // Connections
    private final String address;
    private final Boolean ssl;
    private final String cipher;
    private final String protocol;
    private final String version;
    private final String user;
    private final String keyspace;
    private final String requests;
    private final String driverName;
    private final String driverVersion;
    private final Map<String, String> clientOptions;
    private final String authenticationMode;
    private final String authenticationMetadata;

    // connectedNativeClientsByUser
    private final int totalConnectedClients;

    private Map<String, Integer> connectionsByUser;

    /**
     * Constructs a new {@link ClientStatsResponse} from the configured {@link Builder}.
     *
     * @param builder the builder used to create this object
     */
    @VisibleForTesting
    protected ClientStatsResponse(Builder builder)
    {
        address = builder.address;
        ssl = builder.ssl;
        cipher = builder.cipher;
        protocol = builder.protocol;
        version = builder.version;
        user = builder.user;
        keyspace = builder.keyspace;
        requests = builder.requests;
        driverName = builder.driverName;
        driverVersion = builder.driverVersion;
        clientOptions = builder.clientOptions;
        authenticationMode = builder.authenticationMode;
        authenticationMetadata = builder.authenticationMetadata;
        totalConnectedClients = builder.totalConnectedClients;
        connectionsByUser = builder.connectionsByUser;
        remoteInetAddress = builder.remoteInetAddress;
        protocolVersion = builder.protocolVersion;
        lastSeenTime = builder.lastSeenTime;

    }

    public ClientStatsResponse()
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
        private String address;
        private Boolean ssl;
        private String cipher;
        private String protocol;
        private String version;
        private String user;
        private String keyspace;
        private String requests;
        private String driverName;
        private String driverVersion;
        private Map<String, String> clientOptions;
        private String authenticationMode;
        private String authenticationMetadata;
        private int totalConnectedClients;

        private Map<String, Integer> connectionsByUser;
        private String remoteInetAddress;
        private String protocolVersion;
        private String lastSeenTime;

        /**
         * Sets the {@code remoteInetAddress} and returns a reference to this Builder enabling method chaining.
         *
         * @param remoteInetAddress the {@code remoteInetAddress} to set
         * @return a reference to this Builder
         */
        public Builder remoteInetAddress(String remoteInetAddress)
        {
            this.remoteInetAddress = remoteInetAddress;
            return this;
        }

        /**
         * Sets the {@code protocolVersion} and returns a reference to this Builder enabling method chaining.
         *
         * @param protocolVersion the {@code protocolVersion} to set
         * @return a reference to this Builder
         */
        public Builder protocolVersion(String protocolVersion)
        {
            this.protocolVersion = protocolVersion;
            return this;
        }

        /**
         * Sets the {@code lastSeenTime} and returns a reference to this Builder enabling method chaining.
         *
         * @param lastSeenTime the {@code lastSeenTime} to set
         * @return a reference to this Builder
         */
        public Builder lastSeenTime(String lastSeenTime)
        {
            this.lastSeenTime = lastSeenTime;
            return this;
        }

        /**
         * Sets the {@code address} and returns a reference to this Builder enabling method chaining.
         *
         * @param address the {@code address} to set
         * @return a reference to this Builder
         */
        public Builder address(String address)
        {
            this.address = address;
            return this;
        }

        /**
         * Sets the {@code ssl} and returns a reference to this Builder enabling method chaining.
         *
         * @param ssl the {@code ssl} to set
         * @return a reference to this Builder
         */
        public Builder ssl(Boolean ssl)
        {
            this.ssl = ssl;
            return this;
        }

        /**
         * Sets the {@code cipher} and returns a reference to this Builder enabling method chaining.
         *
         * @param cipher the {@code cipher} to set
         * @return a reference to this Builder
         */
        public Builder cipher(String cipher)
        {
            this.cipher = cipher;
             return this;
        }

        /**
         * Sets the {@code protocol} and returns a reference to this Builder enabling method chaining.
         *
         * @param protocol the {@code protocol} to set
         * @return a reference to this Builder
         */
        public Builder protocol(String protocol)
        {
            this.protocol = protocol;
            return this;
        }

        /**
         * Sets the {@code version} and returns a reference to this Builder enabling method chaining.
         *
         * @param version the {@code version} to set
         * @return a reference to this Builder
         */
        public Builder version(String version)
        {
            this.version = version;
            return this;
        }

        /**
         * Sets the {@code user} and returns a reference to this Builder enabling method chaining.
         *
         * @param user the {@code user} to set
         * @return a reference to this Builder
         */
        public Builder user(String user)
        {
            this.user = user;
            return this;
        }

        /**
         * Sets the {@code keyspace} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace the {@code keyspace} to set
         * @return a reference to this Builder
         */
        public Builder keyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        /**
         * Sets the {@code requests} and returns a reference to this Builder enabling method chaining.
         *
         * @param requests the {@code requests} to set
         * @return a reference to this Builder
         */
        public Builder requests(String requests)
        {
            this.requests = requests;
            return this;
        }

        /**
         * Sets the {@code driverName} and returns a reference to this Builder enabling method chaining.
         *
         * @param driverName the {@code driverName} to set
         * @return a reference to this Builder
         */
        public Builder driverName(String driverName)
        {
            this.driverName = driverName;
            return this;
        }

        /**
         * Sets the {@code driverVersion} and returns a reference to this Builder enabling method chaining.
         *
         * @param driverVersion the {@code driverVersion} to set
         * @return a reference to this Builder
         */
        public Builder driverVersion(String driverVersion)
        {
            this.driverVersion = driverVersion;
            return this;
        }

        /**
         * Sets the {@code clientOptions} and returns a reference to this Builder enabling method chaining.
         *
         * @param clientOptions the {@code clientOptions} to set
         * @return a reference to this Builder
         */
        public Builder clientOptions(Map<String, String> clientOptions)
        {
            this.clientOptions = clientOptions;
            return this;
        }

        /**
         * Sets the {@code authenticationMode} and returns a reference to this Builder enabling method chaining.
         *
         * @param authenticationMode the {@code authenticationMode} to set
         * @return a reference to this Builder
         */
        public Builder authenticationMode(String authenticationMode)
        {
            this.authenticationMode = authenticationMode;
            return this;
        }

        /**
         * Sets the {@code authenticationMetadata} and returns a reference to this Builder enabling method chaining.
         *
         * @param authenticationMetadata the {@code authenticationMetadata} to set
         * @return a reference to this Builder
         */
        public Builder authenticationMetadata(String authenticationMetadata)
        {
            this.authenticationMetadata = authenticationMetadata;
            return this;
        }

        /**
         * Sets the {@code totalConnectedClients} and returns a reference to this Builder enabling method chaining.
         *
         * @param totalConnectedClients the {@code totalConnectedClients} to set
         * @return a reference to this Builder
         */
        public Builder totalConnectedClients(int totalConnectedClients)
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
        public Builder connectionsByUser(Map<String, Integer> connectionsByUser)
        {
            this.connectionsByUser = connectionsByUser;
            return this;
        }

        private Builder()
        {
        }

        public ClientStatsResponse build()
        {
            return new ClientStatsResponse(this);
        }
    }
}
