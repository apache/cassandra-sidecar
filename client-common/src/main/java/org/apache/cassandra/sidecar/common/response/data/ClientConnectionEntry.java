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

package org.apache.cassandra.sidecar.common.response.data;

import com.google.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class representing an entry of the Client Connection Stats
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClientConnectionEntry
{
    @JsonProperty("address")
    public String address()
    {
        return address;
    }

    @JsonProperty("port")
    public int port()
    {
        return port;
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

    @JsonProperty("requests")
    public int requests()
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

    private final String address;
    private final int port;
    private final Boolean ssl;
    private final String cipher;
    private final String protocol;
    private final String version;
    private final String user;
    private final int requests;
    private final String driverName;
    private final String driverVersion;

    /**
     * Constructs a new {@link ClientConnectionEntry} from the configured {@link Builder}.
     *
     * @param builder the builder used to create this object
     */
    @VisibleForTesting
    protected ClientConnectionEntry(Builder builder)
    {
        address = builder.address;
        port = builder.port;
        ssl = builder.ssl;
        cipher = builder.cipher;
        protocol = builder.protocol;
        version = builder.version;
        user = builder.user;
        requests = builder.requests;
        driverName = builder.driverName;
        driverVersion = builder.driverVersion;
    }

    public ClientConnectionEntry()
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
        private int port;
        private Boolean ssl;
        private String cipher;
        private String protocol;
        private String version;
        private String user;
        private int requests;
        private String driverName;
        private String driverVersion;

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
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code address} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            this.port = port;
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
         * Sets the {@code requests} and returns a reference to this Builder enabling method chaining.
         *
         * @param requests the {@code requests} to set
         * @return a reference to this Builder
         */
        public Builder requests(int requests)
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
        
        private Builder()
        {
        }

        public ClientConnectionEntry build()
        {
            return new ClientConnectionEntry(this);
        }
    }
}
