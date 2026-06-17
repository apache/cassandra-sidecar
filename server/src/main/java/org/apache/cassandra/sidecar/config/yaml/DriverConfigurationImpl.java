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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * The driver configuration to use when connecting to Cassandra
 */
public class DriverConfigurationImpl implements DriverConfiguration
{
    private static final int DEFAULT_NUM_CONNECTIONS = 1000;
    private static final SecondBoundConfiguration DEFAULT_UNSUPPORTED_TABLE_SCHEMA_REFRESH_TIME = new SecondBoundConfiguration(5, TimeUnit.MINUTES);

    @JsonProperty("contact_points")
    private final List<InetSocketAddress> contactPoints;

    @JsonProperty("local_dc")
    private final String localDc;

    @JsonProperty("num_connections")
    private final int numConnections;

    @JsonProperty("username")
    private final String username;

    @JsonProperty("password")
    private final String password;

    @JsonProperty("auth_provider")
    private final ParameterizedClassConfiguration authProvider;

    @JsonProperty("ssl")
    private final SslConfiguration sslConfiguration;

    @JsonProperty("unsupported_table_schema_refresh_time")
    private final SecondBoundConfiguration unsupportedTableSchemaRefreshTime;

    public DriverConfigurationImpl()
    {
        this(builder());
    }

    private DriverConfigurationImpl(Builder builder)
    {
        contactPoints = builder.contactPoints;
        localDc = builder.localDc;
        numConnections = builder.numConnections;
        username = builder.username;
        password = builder.password;
        authProvider = builder.authProvider;
        sslConfiguration = builder.sslConfiguration;
        unsupportedTableSchemaRefreshTime = builder.unsupportedTableSchemaRefreshTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("contact_points")
    public List<InetSocketAddress> contactPoints()
    {
        return contactPoints;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("num_connections")
    public int numConnections()
    {
        return numConnections;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("local_dc")
    public String localDc()
    {
        return localDc;
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    @JsonProperty("username")
    public String username()
    {
        return username;
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    @JsonProperty("password")
    public String password()
    {
        return password;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("auth_provider")
    public ParameterizedClassConfiguration authProvider()
    {
        return authProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("ssl")
    public SslConfiguration sslConfiguration()
    {
        return sslConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("unsupported_table_schema_refresh_time")
    public SecondBoundConfiguration unsupportedTableSchemaRefreshTime()
    {
        return unsupportedTableSchemaRefreshTime;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code DriverConfigurationImpl} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, DriverConfigurationImpl>
    {
        private List<InetSocketAddress> contactPoints = List.of();
        private String localDc;
        private int numConnections = DEFAULT_NUM_CONNECTIONS;
        private String username;
        private String password;
        private ParameterizedClassConfiguration authProvider;
        private SslConfiguration sslConfiguration;
        private SecondBoundConfiguration unsupportedTableSchemaRefreshTime = DEFAULT_UNSUPPORTED_TABLE_SCHEMA_REFRESH_TIME;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code contactPoints} and returns a reference to this Builder enabling method chaining.
         *
         * @param contactPoints the {@code contactPoints} to set
         * @return a reference to this Builder
         */
        public Builder contactPoints(List<InetSocketAddress> contactPoints)
        {
            return update(b -> b.contactPoints = contactPoints);
        }

        /**
         * Sets the {@code localDc} and returns a reference to this Builder enabling method chaining.
         *
         * @param localDc the {@code localDc} to set
         * @return a reference to this Builder
         */
        public Builder localDc(String localDc)
        {
            return update(b -> b.localDc = localDc);
        }

        /**
         * Sets the {@code numConnections} and returns a reference to this Builder enabling method chaining.
         *
         * @param numConnections the {@code numConnections} to set
         * @return a reference to this Builder
         */
        public Builder numConnections(int numConnections)
        {
            return update(b -> b.numConnections = numConnections);
        }

        /**
         * Sets the {@code username} and returns a reference to this Builder enabling method chaining.
         *
         * @param username the {@code username} to set
         * @return a reference to this Builder
         * @deprecated use {@link #authProvider(ParameterizedClassConfiguration)} to supply credentials instead
         */
        @Deprecated
        public Builder username(String username)
        {
            return update(b -> b.username = username);
        }

        /**
         * Sets the {@code password} and returns a reference to this Builder enabling method chaining.
         *
         * @param password the {@code password} to set
         * @return a reference to this Builder
         * @deprecated use {@link #authProvider(ParameterizedClassConfiguration)} to supply credentials instead
         */
        @Deprecated
        public Builder password(String password)
        {
            return update(b -> b.password = password);
        }

        /**
         * Sets the {@code authProvider} and returns a reference to this Builder enabling method chaining.
         *
         * <p>The auth provider is the preferred way to configure credentials, replacing the deprecated
         * {@link #username(String)} and {@link #password(String)} setters. For example:
         *
         * <pre>{@code
         * ParameterizedClassConfiguration authProvider =
         *     new ParameterizedClassConfigurationImpl("org.apache.cassandra.sidecar.cluster.auth.ConfigProvider",
         *                                             Map.of(org.apache.cassandra.sidecar.cluster.auth.ConfigProvider.USERNAME_PARAM, "cassandra",
         *                                                    org.apache.cassandra.sidecar.cluster.auth.ConfigProvider.PASSWORD_PARAM, "cassandra"));
         *
         * DriverConfiguration driverConfiguration = DriverConfigurationImpl.builder()
         *                                                                  .authProvider(authProvider)
         *                                                                  ...
         *                                                                  .build();
         * }</pre>
         *
         * @param authProvider the {@code authProvider} to set
         * @return a reference to this Builder
         */
        public Builder authProvider(ParameterizedClassConfiguration authProvider)
        {
            return update(b -> b.authProvider = authProvider);
        }

        /**
         * Sets the {@code sslConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param sslConfiguration the {@code sslConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder sslConfiguration(SslConfiguration sslConfiguration)
        {
            return update(b -> b.sslConfiguration = sslConfiguration);
        }

        /**
         * Sets the {@code unsupportedTableSchemaRefreshTime} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param unsupportedTableSchemaRefreshTime the {@code unsupportedTableSchemaRefreshTime} to set
         * @return a reference to this Builder
         */
        public Builder unsupportedTableSchemaRefreshTime(SecondBoundConfiguration unsupportedTableSchemaRefreshTime)
        {
            return update(b -> b.unsupportedTableSchemaRefreshTime = unsupportedTableSchemaRefreshTime);
        }

        /**
         * Returns a {@code DriverConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code DriverConfigurationImpl} built with parameters of this {@code DriverConfigurationImpl.Builder}
         */
        @Override
        public DriverConfigurationImpl build()
        {
            return new DriverConfigurationImpl(this);
        }
    }
}
