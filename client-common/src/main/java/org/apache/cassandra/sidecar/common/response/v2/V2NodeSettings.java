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

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.response.NodeSettings;

/**
 * V2NodeSettings stores settings information for Cassandra.
 */
public class V2NodeSettings
{
    // Contains Cassandra node settings from system_views.setting table.
    @JsonProperty("nodeSettings")
    private final Map<String, String> nodeSettings;

    /**
     * Constructs a new {@link NodeSettings}.
     */
    public V2NodeSettings()
    {
        this(builder());
    }

    public V2NodeSettings(V2NodeSettings.Builder builder)
    {
        this.nodeSettings = builder.nodeSettings;

    }

    @JsonProperty("nodeSettings")
    public Map<String, String> nodeSettings()
    {
        return nodeSettings;
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
        return Objects.equals(nodeSettings, that.nodeSettings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeSettings);
    }

    /**
     * A builder class to enable construction of V2NodeSettings objects.
     */
    public static final class Builder implements DataObjectBuilder<Builder, V2NodeSettings>
    {
        private Map<String, String> nodeSettings;

        private Builder()
        {
        }

        @Override
        public V2NodeSettings.Builder self()
        {
            return this;
        }

        /**
         * Sets a value for the Cassandra settings map and returns a reference to this Builder enabling method chaining.
         * @param nodeSettings Map of Cassandra setting name to setting value.
         * @return a reference to this Builder
         */
        public V2NodeSettings.Builder nodeSettings(Map<String, String> nodeSettings)
        {
            return update(b -> b.nodeSettings = nodeSettings);
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
