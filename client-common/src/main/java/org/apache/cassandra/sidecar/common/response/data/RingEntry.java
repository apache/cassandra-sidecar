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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class representing an entry in the Cassandra Ring
 */
public class RingEntry
{
    private final String datacenter;
    private final String address;
    private final int port;
    private final String rack;
    private final String status;
    private final String state;
    private final String load;
    private final String owns;
    private final String token;
    private final String fqdn;
    private final String hostId;

    @JsonCreator
    public RingEntry(
    @JsonProperty("datacenter") String datacenter,
    @JsonProperty("address") String address,
    @JsonProperty("port") int port,
    @JsonProperty("rack") String rack,
    @JsonProperty("status") String status,
    @JsonProperty("state") String state,
    @JsonProperty("load") String load,
    @JsonProperty("owns") String owns,
    @JsonProperty("token") String token,
    @JsonProperty("fqdn") String fqdn,
    @JsonProperty("hostId") String hostId)
    {
        this.datacenter = datacenter;
        this.address = address;
        this.port = port;
        this.rack = rack;
        this.status = status;
        this.state = state;
        this.load = load;
        this.owns = owns;
        this.token = token;
        this.fqdn = fqdn;
        this.hostId = hostId;
    }

    private RingEntry(Builder builder)
    {
        datacenter = builder.datacenter;
        address = builder.address;
        port = builder.port;
        rack = builder.rack;
        status = builder.status;
        state = builder.state;
        load = builder.load;
        owns = builder.owns;
        token = builder.token;
        fqdn = builder.fqdn;
        hostId = builder.hostId;
    }

    /**
     * @return data center the node is part of
     */
    @JsonProperty("datacenter")
    public String datacenter()
    {
        return datacenter;
    }

    /**
     * @return address of a node in cassandra ring
     */
    @JsonProperty("address")
    public String address()
    {
        return address;
    }

    /**
     * @return port of a node in cassandra ring
     */
    @JsonProperty("port")
    public int port()
    {
        return port;
    }

    /**
     * @return rack the node is part of
     */
    @JsonProperty("rack")
    public String rack()
    {
        return rack;
    }

    /**
     * @return status of node
     */
    @JsonProperty("status")
    public String status()
    {
        return status;
    }

    /**
     * @return state of node in cassandra ring
     */
    @JsonProperty("state")
    public String state()
    {
        return state;
    }

    /**
     * @return The amount of file system data under the cassandra data directory after excluding all content in the
     * snapshots subdirectories
     */
    @JsonProperty("load")
    public String load()
    {
        return load;
    }

    /**
     * @return token range the node owns
     */
    @JsonProperty("owns")
    public String owns()
    {
        return owns;
    }

    /**
     * @return starting token of the range
     */
    @JsonProperty("token")
    public String token()
    {
        return token;
    }

    /**
     * @return domain name of the node
     */
    @JsonProperty("fqdn")
    public String fqdn()
    {
        return fqdn;
    }

    /**
     * @return identifier for the host
     */
    @JsonProperty("hostId")
    public String hostId()
    {
        return hostId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "RingEntry{" +
               "datacenter='" + datacenter + '\'' +
               ", address='" + address + '\'' +
               ", port=" + port +
               ", rack='" + rack + '\'' +
               ", status='" + status + '\'' +
               ", state='" + state + '\'' +
               ", load='" + load + '\'' +
               ", owns='" + owns + '\'' +
               ", token='" + token + '\'' +
               ", fqdn='" + fqdn + '\'' +
               ", hostId='" + hostId + '\'' +
               '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RingEntry ringEntry = (RingEntry) o;
        return port == ringEntry.port
               && Objects.equals(datacenter, ringEntry.datacenter)
               && Objects.equals(address, ringEntry.address)
               && Objects.equals(rack, ringEntry.rack)
               && Objects.equals(status, ringEntry.status)
               && Objects.equals(state, ringEntry.state)
               && Objects.equals(load, ringEntry.load)
               && Objects.equals(owns, ringEntry.owns)
               && Objects.equals(token, ringEntry.token)
               && Objects.equals(fqdn, ringEntry.fqdn)
               && Objects.equals(hostId, ringEntry.hostId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(datacenter, address, port, rack, status, state, load, owns, token, fqdn, hostId);
    }

    /**
     * {@code RingEntry} builder static inner class.
     */
    public static final class Builder
    {
        private String datacenter;
        private String address;
        private int port;
        private String rack;
        private String status;
        private String state;
        private String load;
        private String owns;
        private String token;
        private String fqdn;
        private String hostId;

        /**
         * Sets the {@code datacenter} and returns a reference to this Builder enabling method chaining.
         *
         * @param datacenter the {@code datacenter} to set
         * @return a reference to this Builder
         */
        public Builder datacenter(String datacenter)
        {
            this.datacenter = datacenter;
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
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            this.port = port;
            return this;
        }

        /**
         * Sets the {@code rack} and returns a reference to this Builder enabling method chaining.
         *
         * @param rack the {@code rack} to set
         * @return a reference to this Builder
         */
        public Builder rack(String rack)
        {
            this.rack = rack;
            return this;
        }

        /**
         * Sets the {@code status} and returns a reference to this Builder enabling method chaining.
         *
         * @param status the {@code status} to set
         * @return a reference to this Builder
         */
        public Builder status(String status)
        {
            this.status = status;
            return this;
        }

        /**
         * Sets the {@code state} and returns a reference to this Builder enabling method chaining.
         *
         * @param state the {@code state} to set
         * @return a reference to this Builder
         */
        public Builder state(String state)
        {
            this.state = state;
            return this;
        }

        /**
         * Sets the {@code load} and returns a reference to this Builder enabling method chaining.
         *
         * @param load the {@code load} to set
         * @return a reference to this Builder
         */
        public Builder load(String load)
        {
            this.load = load;
            return this;
        }

        /**
         * Sets the {@code owns} and returns a reference to this Builder enabling method chaining.
         *
         * @param owns the {@code owns} to set
         * @return a reference to this Builder
         */
        public Builder owns(String owns)
        {
            this.owns = owns;
            return this;
        }

        /**
         * Sets the {@code token} and returns a reference to this Builder enabling method chaining.
         *
         * @param token the {@code token} to set
         * @return a reference to this Builder
         */
        public Builder token(String token)
        {
            this.token = token;
            return this;
        }

        /**
         * Sets the {@code fqdn} (fully qualified domain name) and
         * returns a reference to this Builder enabling method chaining.
         *
         * @param fqdn the {@code fqdn} to set
         * @return a reference to this Builder
         */
        public Builder fqdn(String fqdn)
        {
            this.fqdn = fqdn;
            return this;
        }

        /**
         * Sets the {@code hostId} and returns a reference to this Builder enabling method chaining.
         *
         * @param hostId the {@code hostId} to set
         * @return a reference to this Builder
         */
        public Builder hostId(String hostId)
        {
            this.hostId = hostId;
            return this;
        }

        /**
         * Returns a {@code RingEntry} built from the parameters previously set.
         *
         * @return a {@code RingEntry} built with parameters of this {@code RingEntry.Builder}
         */
        public RingEntry build()
        {
            return new RingEntry(this);
        }
    }
}

