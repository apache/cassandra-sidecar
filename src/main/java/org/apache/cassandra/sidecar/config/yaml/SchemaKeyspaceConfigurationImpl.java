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

package org.apache.cassandra.sidecar.config.yaml;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;

/**
 * Configuration for sidecar schema creation
 */
@Binds(to = SchemaKeyspaceConfiguration.class)
public class SchemaKeyspaceConfigurationImpl implements SchemaKeyspaceConfiguration
{
    public static final boolean DEFAULT_IS_ENABLED = false;
    public static final String DEFAULT_KEYSPACE = "sidecar_internal";
    public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
    public static final int DEFAULT_REPLICATION_FACTOR = 1;

    @JsonProperty(value = "is_enabled", defaultValue = "false")
    protected final boolean isEnabled;

    @JsonProperty(value = "keyspace", defaultValue = "sidecar_internal")
    protected final String keyspace;

    @JsonProperty(value = "replication_strategy", defaultValue = "SimpleStrategy")
    protected final String replicationStrategy;

    @JsonProperty(value = "replication_factor", defaultValue = "1")
    protected final int replicationFactor;

    protected SchemaKeyspaceConfigurationImpl()
    {
        this(builder());
    }

    protected SchemaKeyspaceConfigurationImpl(Builder builder)
    {
        this.isEnabled = builder.isEnabled;
        this.keyspace = builder.keyspace;
        this.replicationStrategy = builder.replicationStrategy;
        this.replicationFactor = builder.replicationFactor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "is_enabled", defaultValue = "false")
    public boolean isEnabled()
    {
        return isEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "keyspace", defaultValue = "sidecar_internal")
    public String keyspace()
    {
        return keyspace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "replication_strategy", defaultValue = "SimpleStrategy")
    public String replicationStrategy()
    {
        return replicationStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "replication_factor", defaultValue = "1")
    public int replicationFactor()
    {
        return replicationFactor;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code SchemaKeyspaceConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, SchemaKeyspaceConfigurationImpl>
    {
        private boolean isEnabled = DEFAULT_IS_ENABLED;
        private String keyspace = DEFAULT_KEYSPACE;
        private String replicationStrategy = DEFAULT_REPLICATION_STRATEGY;
        private int replicationFactor = DEFAULT_REPLICATION_FACTOR;

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code isEnabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param isEnabled the {@code isEnabled} to set
         * @return a reference to this Builder
         */
        public Builder isEnabled(boolean isEnabled)
        {
            this.isEnabled = isEnabled;
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
         * Sets the {@code replicationStrategy} and returns a reference to this Builder enabling method chaining.
         *
         * @param replicationStrategy the {@code replicationStrategy} to set
         * @return a reference to this Builder
         */
        public Builder replicationStrategy(String replicationStrategy)
        {
            this.replicationStrategy = replicationStrategy;
            return this;
        }

        /**
         * Sets the {@code replicationFactor} and returns a reference to this Builder enabling method chaining.
         *
         * @param replicationFactor the {@code replicationFactor} to set
         * @return a reference to this Builder
         */
        public Builder replicationFactor(int replicationFactor)
        {
            this.replicationFactor = replicationFactor;
            return this;
        }

        @Override
        public SchemaKeyspaceConfigurationImpl build()
        {
            return new SchemaKeyspaceConfigurationImpl(this);
        }
    }
}
