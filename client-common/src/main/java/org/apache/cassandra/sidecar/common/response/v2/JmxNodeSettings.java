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
package org.apache.cassandra.sidecar.common.response.v2;

import java.net.InetAddress;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * JmxNodeSettings stores the settings which are accessible via JMX through the
 * org.apache.cassandra.db:type=StorageService and org.apache.cassandra.db:type=EndpointSnitchInfo MBeans. This maintains
 * compatability with the /api/v1/cassandra/settings object, expect the sidecarVersion has been relocated.
 */
public class JmxNodeSettings
{
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

    public JmxNodeSettings()
    {
        this(builder());
    }

    protected JmxNodeSettings(Builder builder)
    {
        this.releaseVersion = builder.releaseVersion;
        this.partitioner = builder.partitioner;
        this.datacenter = builder.datacenter;
        this.rpcAddress = builder.rpcAddress;
        this.rpcPort = builder.rpcPort;
        this.tokens = builder.tokens;
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

    public static JmxNodeSettings.Builder builder()
    {
        return new JmxNodeSettings.Builder();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        JmxNodeSettings that = (JmxNodeSettings) o;
        return rpcPort == that.rpcPort && Objects.equals(releaseVersion, that.releaseVersion) && Objects.equals(partitioner, that.partitioner) && Objects.equals(datacenter, that.datacenter) && Objects.equals(rpcAddress, that.rpcAddress) && Objects.equals(tokens, that.tokens);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(releaseVersion, partitioner, datacenter, rpcAddress, rpcPort, tokens);
    }

    /**
     * A Builder to help construct JmxNodeSettings objects.
     */
    public static final class Builder implements DataObjectBuilder<Builder, JmxNodeSettings>
    {
        private String releaseVersion;
        private String partitioner;
        private String datacenter;
        private InetAddress rpcAddress;
        private int rpcPort;
        private Set<String> tokens;

        private Builder(){}

        @Override
        public Builder self()
        {
            return this;
        }

        @Override
        public JmxNodeSettings build()
        {
            return new JmxNodeSettings(this);
        }

        public Builder releaseVersion(String releaseVersion)
        {
            return update(b -> b.releaseVersion = releaseVersion);
        }

        public Builder partitioner(String partitioner)
        {
            return update(b -> b.partitioner = partitioner);
        }

        public Builder datacenter(String datacenter)
        {
            return update(b -> b.datacenter = datacenter);
        }

        public Builder rpcAddress(InetAddress rpcAddress)
        {
            return update(b -> b.rpcAddress = rpcAddress);
        }

        public Builder rpcPort(int rpcPort)
        {
            return update(b -> b.rpcPort = rpcPort);
        }

        public Builder tokens(Set<String> tokens)
        {
            return update(b -> b.tokens = tokens);
        }
    }
}
