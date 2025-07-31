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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.response.NodeSettings;

/**
 * V2NodeSettings stores settings information for Cassandra and Sidecar instances.
 */
public class V2NodeSettings
{
    private static final String VERSION = "version";
    // Contains settings about this Cassandra Sidecar instance (e.g. version)
    @JsonProperty("sidecar")
    private final Map<String, String> sidecar;

    //Contains settings which can be retrieved through JMX.
    @JsonProperty("jmx")
    private final JmxNodeSettings jmx;

    // Contains Cassandra node settings from system_views.settigns table.
    @JsonProperty("cassandra")
    private final Map<String, String> cassandra;

    /**
     * Constructs a new {@link NodeSettings}.
     */
    public V2NodeSettings()
    {
        this(builder());
    }

    public V2NodeSettings(V2NodeSettings.Builder builder)
    {
        this.cassandra = builder.cassandra;
        this.sidecar = builder.sidecar != null ? builder.sidecar : Collections.emptyMap();
        this.jmx = builder.jmx;

    }

    @JsonProperty("cassandra")
    public Map<String, String> cassandra()
    {
        return cassandra;
    }

    @JsonProperty("jmx")
    public JmxNodeSettings jmx()
    {
        return jmx;
    }

    @JsonProperty("sidecar")
    public Map<String, String> sidecar()
    {
        return sidecar;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        V2NodeSettings that = (V2NodeSettings) o;
        return Objects.equals(sidecar, that.sidecar) && Objects.equals(jmx, that.jmx) && Objects.equals(cassandra, that.cassandra);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sidecar, jmx, cassandra);
    }

    /**
     * A builder class to enable construction of V2NodeSettings objects.
     */
    public static final class Builder implements DataObjectBuilder<Builder, V2NodeSettings>
    {
        private JmxNodeSettings jmx;
        private Map<String, String> sidecar;
        private Map<String, String> cassandra;

        private Builder()
        {
        }

        @Override
        public V2NodeSettings.Builder self()
        {
            return this;
        }

        /**
         * Sets the Cassandra node settings which can be retrieved from Gossip (e.g. Partitioner, release version, tokens)
         * @param jmx the node settings retrieved from jmx
         * @return a reference to this builder.
         */
        public V2NodeSettings.Builder jmx(JmxNodeSettings jmx)
        {
            return update(b -> b.jmx = jmx);
        }

        public V2NodeSettings.Builder sidecar(Map<String, String> sidecar)
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
        public V2NodeSettings.Builder sidecarVersion(String sidecarVersion)
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
         * Sets a value for the Cassandra settings map and returns a reference to this Builder enabling method chaining.
         * @param cassandra
         * @return a reference to this Builder
         */
        public V2NodeSettings.Builder cassandra(Map<String, String> cassandra)
        {
            return update(b -> b.cassandra = cassandra);
        }

        /**
         * Returns a {@code NodeSettings} built from the parameters previously set.
         *
         * @return a {@code NodeSettings} built with parameters of this {@code NodeSettings.Builder}
         */
        @Override
        public V2NodeSettings build()
        {
            return new V2NodeSettings(this);
        }
    }
}
