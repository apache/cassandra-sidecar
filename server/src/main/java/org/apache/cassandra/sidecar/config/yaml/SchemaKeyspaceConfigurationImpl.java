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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * Configuration for sidecar schema creation
 */
public class SchemaKeyspaceConfigurationImpl implements SchemaKeyspaceConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaKeyspaceConfigurationImpl.class);
    public static final boolean DEFAULT_IS_ENABLED = false;
    public static final String DEFAULT_KEYSPACE = "sidecar_internal";
    public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
    public static final int DEFAULT_REPLICATION_FACTOR = 1;
    public static final SecondBoundConfiguration DEFAULT_LEASE_SCHEMA_TTL = SecondBoundConfiguration.parse("2m");
    public static final SecondBoundConfiguration MINIMUM_LEASE_SCHEMA_TTL = SecondBoundConfiguration.parse("1m");

    @JsonProperty(value = "is_enabled")
    protected final boolean isEnabled;

    @JsonProperty(value = "keyspace")
    protected final String keyspace;

    @JsonProperty(value = "replication_strategy")
    protected final String replicationStrategy;

    @JsonProperty(value = "replication_factor")
    protected final int replicationFactor;

    protected SecondBoundConfiguration leaseSchemaTTL;

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
        this.leaseSchemaTTL = builder.leaseSchemaTTL;
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
    @JsonProperty(value = "lease_schema_ttl")
    public SecondBoundConfiguration leaseSchemaTTL()
    {
        return leaseSchemaTTL;
    }

    @JsonProperty(value = "lease_schema_ttl")
    public void setLeaseSchemaTTL(SecondBoundConfiguration leaseSchemaTTL)
    {
        if (leaseSchemaTTL.compareTo(MINIMUM_LEASE_SCHEMA_TTL) < 0)
        {
            throw leaseTTLConfigurationException(leaseSchemaTTL);
        }
        this.leaseSchemaTTL = leaseSchemaTTL;
    }

    /**
     * Legacy property {@code lease_schema_ttl_sec}
     *
     * @param leaseSchemaTTLSeconds time-to-live in seconds
     * @deprecated in favor of {@code lease_schema_ttl}
     */
    @JsonProperty(value = "lease_schema_ttl_sec")
    @Deprecated
    public void setLeaseSchemaTTLSeconds(long leaseSchemaTTLSeconds)
    {
        LOGGER.warn("'lease_schema_ttl_sec' is deprecated, use 'lease_schema_ttl' instead");
        setLeaseSchemaTTL(new SecondBoundConfiguration(leaseSchemaTTLSeconds, TimeUnit.SECONDS));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    static ConfigurationException leaseTTLConfigurationException(SecondBoundConfiguration leaseSchemaTTL)
    {
        String message = String.format("Lease schema TTL value of %s is less than the minimum allowed value of %s",
                                       leaseSchemaTTL, MINIMUM_LEASE_SCHEMA_TTL);
        return new ConfigurationException(message);
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
        private SecondBoundConfiguration leaseSchemaTTL = DEFAULT_LEASE_SCHEMA_TTL;

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
         * Sets the {@code leaseSchemaTTL} and returns a reference to this Builder enabling method chaining.
         *
         * @param leaseSchemaTTL the {@code leaseSchemaTTL} to set
         * @return a reference to this Builder
         */
        public Builder leaseSchemaTTL(SecondBoundConfiguration leaseSchemaTTL)
        {
            this.leaseSchemaTTL = leaseSchemaTTL;
            return this;
        }

        @Override
        public SchemaKeyspaceConfigurationImpl build()
        {
            if (leaseSchemaTTL.compareTo(MINIMUM_LEASE_SCHEMA_TTL) < 0)
            {
                throw leaseTTLConfigurationException(leaseSchemaTTL);
            }
            return new SchemaKeyspaceConfigurationImpl(this);
        }
    }
}
