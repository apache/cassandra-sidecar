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

    /* SSL related settings */
    private final String keyStorePath;
    private final String keyStorePassword;
    private final boolean isSslEnabled;

    /**
     * Constructor
     *
     * @param cassandraHost
     * @param cassandraPort
     * @param port
     * @param healthCheckFrequencyMillis
     */
    public Configuration(String cassandraHost, Integer cassandraPort, String host, Integer port,
                         Integer healthCheckFrequencyMillis, String keyStorePath, String keyStorePassword,
                         boolean isSslEnabled)
    {
        this.cassandraHost = cassandraHost;
        this.cassandraPort = cassandraPort;
        this.host = host;
        this.port = port;
        this.healthCheckFrequencyMillis = healthCheckFrequencyMillis;

        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.isSslEnabled = isSslEnabled;
    }

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
    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    /**
     * Get the Keystore password
     *
     * @return
     */
    public String getKeystorePassword()
    {
        return keyStorePassword;
    }
}
