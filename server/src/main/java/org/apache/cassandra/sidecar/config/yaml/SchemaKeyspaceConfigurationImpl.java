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

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * Configuration for sidecar schema creation
 */
public class SchemaKeyspaceConfigurationImpl implements SchemaKeyspaceConfiguration
{
    public static final boolean DEFAULT_IS_ENABLED = false;
    public static final String DEFAULT_KEYSPACE = "sidecar_internal";
    public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
    public static final int DEFAULT_REPLICATION_FACTOR = 1;
    public static final long DEFAULT_LEASE_SCHEMA_TTL_SECONDS = TimeUnit.MINUTES.toSeconds(2);
    public static final long MINIMUM_LEASE_SCHEMA_TTL_SECONDS = TimeUnit.MINUTES.toSeconds(1);

    @JsonProperty(value = "is_enabled")
    protected final boolean isEnabled;

    @JsonProperty(value = "keyspace")
    protected final String keyspace;

    @JsonProperty(value = "replication_strategy")
    protected final String replicationStrategy;

    @JsonProperty(value = "replication_factor")
    protected final int replicationFactor;

    protected long leaseSchemaTTLSeconds;

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
        this.leaseSchemaTTLSeconds = builder.leaseSchemaTTLSeconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "is_enabled")
    public boolean isEnabled()
    {
        return isEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "keyspace")
    public String keyspace()
    {
        return keyspace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "replication_strategy")
    public String replicationStrategy()
    {
        return replicationStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "replication_factor")
    public int replicationFactor()
    {
        return replicationFactor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "lease_schema_ttl_sec")
    public long leaseSchemaTTLSeconds()
    {
        return leaseSchemaTTLSeconds;
    }

    @JsonProperty(value = "lease_schema_ttl_sec")
    public void setLeaseSchemaTTLSeconds(long leaseSchemaTTLSeconds)
    {
        if (leaseSchemaTTLSeconds < MINIMUM_LEASE_SCHEMA_TTL_SECONDS)
        {
            throw leaseTTLConfigurationException(leaseSchemaTTLSeconds);
        }
        this.leaseSchemaTTLSeconds = leaseSchemaTTLSeconds;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    static ConfigurationException leaseTTLConfigurationException(long leaseSchemaTTLSeconds)
    {
        return new ConfigurationException(String.format("Lease schema TTL value of '%d' seconds is less than the minimum allowed value of '%d'",
                                                        leaseSchemaTTLSeconds, MINIMUM_LEASE_SCHEMA_TTL_SECONDS));
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
        private long leaseSchemaTTLSeconds = DEFAULT_LEASE_SCHEMA_TTL_SECONDS;

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

        /**
         * Sets the {@code leaseSchemaTTLSeconds} and returns a reference to this Builder enabling method chaining.
         *
         * @param leaseSchemaTTLSeconds the {@code leaseSchemaTTLSeconds} to set
         * @return a reference to this Builder
         */
        public Builder leaseSchemaTTLSeconds(long leaseSchemaTTLSeconds)
        {
            this.leaseSchemaTTLSeconds = leaseSchemaTTLSeconds;
            return this;
        }

        @Override
        public SchemaKeyspaceConfigurationImpl build()
        {
            if (leaseSchemaTTLSeconds < MINIMUM_LEASE_SCHEMA_TTL_SECONDS)
            {
                throw leaseTTLConfigurationException(leaseSchemaTTLSeconds);
            }
            return new SchemaKeyspaceConfigurationImpl(this);
        }
    }
}
