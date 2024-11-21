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
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * The driver configuration to use when connecting to Cassandra
 */
public class DriverConfigurationImpl implements DriverConfiguration
{
    private static final int DEFAULT_NUM_CONNECTIONS = 1000;
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

    @JsonProperty("ssl")
    private final SslConfiguration sslConfiguration;

    public DriverConfigurationImpl()
    {
        this(Collections.emptyList(), null, DEFAULT_NUM_CONNECTIONS, null, null, null);
    }

    public DriverConfigurationImpl(List<InetSocketAddress> contactPoints,
                                   String localDc,
                                   int numConnections,
                                   String username,
                                   String password,
                                   SslConfiguration sslConfiguration)
    {
        this.contactPoints = contactPoints;
        this.localDc = localDc;
        this.numConnections = numConnections;
        this.username = username;
        this.password = password;
        this.sslConfiguration = sslConfiguration;
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
    @Override
    @JsonProperty("username")
    public String username()
    {
        return username;
    }

    /**
     * {@inheritDoc}
     */
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
    @JsonProperty("ssl")
    public SslConfiguration sslConfiguration()
    {
        return sslConfiguration;
    }
}
