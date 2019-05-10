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

package org.apache.cassandra.sidecar;

import javax.annotation.Nullable;

/**
 * Sidecar configuration
 */
public class Configuration
{
    /* Cassandra Host */
    private final String cassandraHost;

    /* Cassandra Port */
    private final Integer cassandraPort;

    /* Sidecar's HTTP REST API port */
    private final Integer port;

    /* Sidecar's listen address */
    private String host;

    /* Healthcheck frequency in miilis */
    private final Integer healthCheckFrequencyMillis;

    @Nullable
    private final String cassandraUsername;

    @Nullable
    private final String cassandraPassword;

    /* SSL related settings */
    private final boolean isSslEnabled;
    @Nullable
    private final String keyStorePath;
    @Nullable
    private final String keyStorePassword;
    @Nullable
    private final String trustStorePath;
    @Nullable
    private final String trustStorePassword;

    private final boolean isCassandraSslEnabled;
    @Nullable
    private final String cassandraTrustStorePath;
    @Nullable
    private final String cassandraTrustStorePassword;

    // auto generated constructor and builder, disable checkstyle for wide rows

    //CHECKSTYLE:OFF
    private Configuration(String cassandraHost, Integer cassandraPort, Integer port, String host, Integer healthCheckFrequencyMillis, @Nullable String cassandraUsername, @Nullable String cassandraPassword, boolean isSslEnabled, @Nullable String keyStorePath, @Nullable String keyStorePassword, @Nullable String trustStorePath, @Nullable String trustStorePassword, boolean isCassandraSslEnabled, @Nullable String cassandraTrustStorePath, @Nullable String cassandraTrustStorePassword)
    {
        this.cassandraHost = cassandraHost;
        this.cassandraPort = cassandraPort;
        this.port = port;
        this.host = host;
        this.healthCheckFrequencyMillis = healthCheckFrequencyMillis;
        this.cassandraUsername = cassandraUsername;
        this.cassandraPassword = cassandraPassword;
        this.isSslEnabled = isSslEnabled;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.isCassandraSslEnabled = isCassandraSslEnabled;
        this.cassandraTrustStorePath = cassandraTrustStorePath;
        this.cassandraTrustStorePassword = cassandraTrustStorePassword;
    }
    //CHECKSTYLE:ON

    /**
     * Get the Cassandra host
     *
     * @return
     */
    public String getCassandraHost()
    {
        return cassandraHost;
    }

    /**
     * Get the Cassandra port
     *
     * @return
     */
    public Integer getCassandraPort()
    {
        return cassandraPort;
    }

    /**
     *  Sidecar's listen address
     *
     * @return
     */
    public String getHost()
    {
        return host;
    }

    /**
     * Get the Sidecar's REST HTTP API port
     *
     * @return
     */
    public Integer getPort()
    {
        return port;
    }

    /**
     * Get the health check frequency in millis
     *
     * @return
     */
    public Integer getHealthCheckFrequencyMillis()
    {
        return healthCheckFrequencyMillis;
    }

    /**
     * Get the SSL status
     *
     * @return
     */
    public boolean isSslEnabled()
    {
        return isSslEnabled;
    }

    /**
     * Get the Keystore Path
     *
     * @return
     */
    @Nullable
    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    /**
     * Get the Keystore password
     *
     * @return
     */
    @Nullable
    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    /**
     * Get the Truststore Path
     *
     * @return
     */
    @Nullable
    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    /**
     * Get the Truststore password
     *
     * @return
     */
    @Nullable
    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    /**
     * username for cql auth if necessary
     */
    @Nullable
    public String getCassandraUsername()
    {
        return cassandraUsername;
    }

    /**
     * password for cql auth if necessary
     */
    @Nullable
    public String getCassandraPassword()
    {
        return cassandraPassword;
    }

    /**
     * Use SSL for CQL
     */
    public boolean isCassandraSslEnabled()
    {
        return isCassandraSslEnabled;
    }

    /**
     * Truststore location for cql connection
     */
    @Nullable
    public String getCassandraTrustStorePath()
    {
        return cassandraTrustStorePath;
    }

    /**
     * CQL truststore password
     */
    @Nullable
    public String getCassandraTrustStorePassword()
    {
        return cassandraTrustStorePassword;
    }

    //CHECKSTYLE:OFF
    public static final class Builder
    {
        private String cassandraHost;
        private Integer cassandraPort;
        private Integer port;
        private String host;
        private Integer healthCheckFrequencyMillis;
        private String cassandraUsername;
        private String cassandraPassword;
        private boolean isSslEnabled;
        private String keyStorePath;
        private String keyStorePassword;
        private String trustStorePath;
        private String trustStorePassword;
        private boolean isCassandraSslEnabled;
        private String cassandraTrustStorePath;
        private String cassandraTrustStorePassword;

        public Builder()
        {
        }

        public Builder withCassandraHost(String cassandraHost)
        {
            this.cassandraHost = cassandraHost;
            return this;
        }

        public Builder withCassandraPort(Integer cassandraPort)
        {
            this.cassandraPort = cassandraPort;
            return this;
        }

        public Builder withPort(Integer port)
        {
            this.port = port;
            return this;
        }

        public Builder withHost(String host)
        {
            this.host = host;
            return this;
        }

        public Builder withHealthCheckFrequencyMillis(Integer healthCheckFrequencyMillis)
        {
            this.healthCheckFrequencyMillis = healthCheckFrequencyMillis;
            return this;
        }

        public Builder withCassandraUsername(String cassandraUsername)
        {
            this.cassandraUsername = cassandraUsername;
            return this;
        }

        public Builder withCassandraPassword(String cassandraPassword)
        {
            this.cassandraPassword = cassandraPassword;
            return this;
        }

        public Builder withSslEnabled(boolean isSslEnabled)
        {
            this.isSslEnabled = isSslEnabled;
            return this;
        }

        public Builder withKeyStorePath(String keyStorePath)
        {
            this.keyStorePath = keyStorePath;
            return this;
        }

        public Builder withKeyStorePassword(String keyStorePassword)
        {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        public Builder withTrustStorePath(String trustStorePath)
        {
            this.trustStorePath = trustStorePath;
            return this;
        }

        public Builder withTrustStorePassword(String trustStorePassword)
        {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        public Builder withCassandraSslEnabled(boolean isCassandraSslEnabled)
        {
            this.isCassandraSslEnabled = isCassandraSslEnabled;
            return this;
        }

        public Builder withCassandraTrustStorePath(String cassandraTrustStorePath)
        {
            this.cassandraTrustStorePath = cassandraTrustStorePath;
            return this;
        }

        public Builder withCassandraTrustStorePassword(String cassandraTrustStorePassword)
        {
            this.cassandraTrustStorePassword = cassandraTrustStorePassword;
            return this;
        }

        public Configuration build()
        {
            return new Configuration(cassandraHost, cassandraPort, port, host, healthCheckFrequencyMillis, cassandraUsername, cassandraPassword, isSslEnabled, keyStorePath, keyStorePassword, trustStorePath, trustStorePassword, isCassandraSslEnabled, cassandraTrustStorePath, cassandraTrustStorePassword);
        }
        //CHECKSTYLE:ON
    }

}
