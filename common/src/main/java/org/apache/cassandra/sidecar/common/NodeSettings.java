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

package org.apache.cassandra.sidecar.common;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holds information about the specific node settings
 */
public class NodeSettings
{
    private static final String VERSION = "version";

    public static final String RELEASE_VERSION_COLUMN_NAME = "release_version";
    public static final String PARTITIONER_COLUMN_NAME = "partitioner";
    public static final String DATA_CENTER_COLUMN_NAME = "data_center";
    public static final String RPC_ADDRESS_COLUMN_NAME = "rpc_address";
    public static final String RPC_PORT_COLUMN_NAME = "rpc_port";
    public static final String TOKENS_COLUMN_NAME = "tokens";

    @JsonProperty("releaseVersion")
    private final String releaseVersion;
    @JsonProperty("partitioner")
    private final String partitioner;
    @JsonProperty("datacenter")
    private final String datacenter;
    @JsonProperty("rpcAddress")
    private final InetAddress rpcAddress;
    @JsonProperty("rpcPort")
    private final int rpcPort;
    @JsonProperty("tokens")
    private final Set<String> tokens;
    @JsonProperty("sidecar")
    private final Map<String, String> sidecar;

    /**
     * Constructs a new {@link NodeSettings}.
     */
    public NodeSettings()
    {
        this(builder());
    }

    /**
     * Constructs a new {@link NodeSettings} from the configured {@link Builder}.
     *
     * @param builder the builder used to create this object
     */
    protected NodeSettings(Builder builder)
    {
        releaseVersion = builder.releaseVersion;
        partitioner = builder.partitioner;
        datacenter = builder.datacenter;
        rpcAddress = builder.rpcAddress;
        rpcPort = builder.rpcPort;
        tokens = builder.tokens;
        sidecar = builder.sidecar;
    }

    @JsonProperty("releaseVersion")
    public String releaseVersion()
    {
        return releaseVersion;
    }

    @JsonProperty("partitioner")
    public String partitioner()
    {
        return partitioner;
    }

    @JsonProperty("sidecar")
    public Map<String, String> sidecar()
    {
        return sidecar;
    }

    public String sidecarVersion()
    {
        return sidecar != null ? sidecar.get(VERSION) : "unknown";
    }

    @JsonProperty("datacenter")
    public String datacenter()
    {
        return datacenter;
    }

    @JsonProperty("rpcAddress")
    public InetAddress rpcAddress()
    {
        return rpcAddress;
    }

    @JsonProperty("rpcPort")
    public int rpcPort()
    {
        return rpcPort;
    }

    @JsonProperty("tokens")
    public Set<String> tokens()
    {
        return tokens;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }
        NodeSettings that = (NodeSettings) other;
        return Objects.equals(this.releaseVersion, that.releaseVersion)
               && Objects.equals(this.partitioner, that.partitioner)
               && Objects.equals(this.sidecar, that.sidecar)
               && Objects.equals(this.datacenter, that.datacenter)
               && Objects.equals(this.rpcAddress, that.rpcAddress)
               && Objects.equals(this.rpcPort, that.rpcPort)
               && Objects.equals(this.tokens, that.tokens)
        ;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(releaseVersion, partitioner, sidecar, datacenter, rpcAddress, rpcPort, tokens);
    }

    /**
     * @return a new NodeSettings builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code NodeSettings} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, NodeSettings>
    {
        private String releaseVersion;
        private String partitioner;
        private String datacenter;
        private InetAddress rpcAddress;
        private int rpcPort;
        private Set<String> tokens;
        private Map<String, String> sidecar;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code releaseVersion} and returns a reference to this Builder enabling method chaining.
         *
         * @param releaseVersion the {@code releaseVersion} to set
         * @return a reference to this Builder
         */
        public Builder releaseVersion(String releaseVersion)
        {
            return update(b -> b.releaseVersion = releaseVersion);
        }

        /**
         * Sets the {@code partitioner} and returns a reference to this Builder enabling method chaining.
         *
         * @param partitioner the {@code partitioner} to set
         * @return a reference to this Builder
         */
        public Builder partitioner(String partitioner)
        {
            return update(b -> b.partitioner = partitioner);
        }

        /**
         * Sets the {@code datacenter} and returns a reference to this Builder enabling method chaining.
         *
         * @param datacenter the {@code datacenter} to set
         * @return a reference to this Builder
         */
        public Builder datacenter(String datacenter)
        {
            return update(b -> b.datacenter = datacenter);
        }

        /**
         * Sets the {@code rpcAddress} and returns a reference to this Builder enabling method chaining.
         *
         * @param rpcAddress the {@code rpcAddress} to set
         * @return a reference to this Builder
         */
        public Builder rpcAddress(InetAddress rpcAddress)
        {
            return update(b -> b.rpcAddress = rpcAddress);
        }

        /**
         * Sets the {@code rpcPort} and returns a reference to this Builder enabling method chaining.
         *
         * @param rpcPort the {@code rpcPort} to set
         * @return a reference to this Builder
         */
        public Builder rpcPort(int rpcPort)
        {
            return update(b -> b.rpcPort = rpcPort);
        }

        /**
         * Sets the {@code tokens} and returns a reference to this Builder enabling method chaining.
         *
         * @param tokens the {@code tokens} to set
         * @return a reference to this Builder
         */
        public Builder tokens(Set<String> tokens)
        {
            return update(b -> b.tokens = tokens);
        }

        /**
         * Sets the {@code sidecar} and returns a reference to this Builder enabling method chaining.
         *
         * @param sidecar the {@code sidecar} to set
         * @return a reference to this Builder
         */
        public Builder sidecar(Map<String, String> sidecar)
        {
            return update(b -> b.sidecar = sidecar);
        }

        /**
         * Sets the {@code sidecarVersion} in the {@code sidecar} map and returns a reference to this Builder
         * enabling method chaining.
         *
         * @param sidecarVersion the {@code sidecarVersion} to set
         * @return a reference to this Builder
         */
        public Builder sidecarVersion(String sidecarVersion)
        {
            return update(b -> {
                if (b.sidecar != null)
                {
                    b.sidecar.put(VERSION, sidecarVersion);
                }
                else
                {
                    b.sidecar = Collections.singletonMap(VERSION, sidecarVersion);
                }
            });
        }

        /**
         * Returns a {@code NodeSettings} built from the parameters previously set.
         *
         * @return a {@code NodeSettings} built with parameters of this {@code NodeSettings.Builder}
         */
        @Override
        public NodeSettings build()
        {
            return new NodeSettings(this);
        }
    }
}
