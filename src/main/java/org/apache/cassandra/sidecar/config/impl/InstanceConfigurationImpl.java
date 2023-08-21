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

package org.apache.cassandra.sidecar.config.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;

/**
 * Encapsulates the basic configuration needed to connect to a single Cassandra instance
 */
public class InstanceConfigurationImpl implements InstanceConfiguration
{
    @JsonProperty("id")
    private final int id;

    @JsonProperty("host")
    private final String host;

    @JsonProperty("port")
    private final int port;

    @JsonProperty("username")
    private final String username;

    @JsonProperty("password")
    private final String password;

    @JsonProperty("data_dirs")
    private final List<String> dataDirs;

    @JsonProperty("staging_dir")
    private final String stagingDir;

    @JsonProperty("jmx_host")
    private final String jmxHost;

    @JsonProperty("jmx_port")
    private final int jmxPort;

    @JsonProperty("jmx_ssl_enabled")
    private final boolean jmxSslEnabled;

    @JsonProperty("jmx_role")
    private final String jmxRole;

    @JsonProperty("jmx_role_password")
    private final String jmxRolePassword;

    public InstanceConfigurationImpl()
    {
        this.id = 0;
        this.host = null;
        this.port = 0;
        this.username = null;
        this.password = null;
        this.dataDirs = null;
        this.stagingDir = null;
        this.jmxHost = null;
        this.jmxPort = 0;
        this.jmxSslEnabled = false;
        this.jmxRole = null;
        this.jmxRolePassword = null;
    }

    protected InstanceConfigurationImpl(Builder<?> builder)
    {
        id = builder.id;
        host = builder.host;
        port = builder.port;
        username = builder.username;
        password = builder.password;
        dataDirs = Collections.unmodifiableList(builder.dataDirs);
        stagingDir = builder.stagingDir;
        jmxHost = builder.jmxHost;
        jmxPort = builder.jmxPort;
        jmxSslEnabled = builder.jmxSslEnabled;
        jmxRole = builder.jmxRole;
        jmxRolePassword = builder.jmxRolePassword;
    }

    /**
     * @return an identifier for the Cassandra instance
     */
    @Override
    @JsonProperty("id")
    public int id()
    {
        return id;
    }

    /**
     * @return the host address for the Cassandra instance
     */
    @Override
    @JsonProperty("host")
    public String host()
    {
        return host;
    }

    /**
     * @return the port number for the Cassandra instance
     */
    @Override
    @JsonProperty("port")
    public int port()
    {
        return port;
    }

    /**
     * @return the username used for connecting to the Cassandra instance
     */
    @Override
    @JsonProperty("username")
    public String username()
    {
        return username;
    }

    /**
     * @return the password used for connecting to the Cassandra instance
     */
    @Override
    @JsonProperty("password")
    public String password()
    {
        return password;
    }

    /**
     * @return a list of data directories of cassandra instance
     */
    @Override
    @JsonProperty("data_dirs")
    public List<String> dataDirs()
    {
        return dataDirs;
    }

    /**
     * @return staging directory for the uploads of the cassandra instance
     */
    @Override
    @JsonProperty("staging_dir")
    public String stagingDir()
    {
        return stagingDir;
    }

    /**
     * @return the host address of the JMX service for the Cassandra instance
     */
    @Override
    @JsonProperty("jmx_host")
    public String jmxHost()
    {
        return jmxHost;
    }

    /**
     * @return the port number for the JMX service for the Cassandra instance
     */
    @Override
    @JsonProperty("jmx_port")
    public int jmxPort()
    {
        return jmxPort;
    }

    /**
     * @return the port number of the Cassandra instance
     */
    @Override
    @JsonProperty("jmx_ssl_enabled")
    public boolean jmxSslEnabled()
    {
        return jmxSslEnabled;
    }

    /**
     * @return the name of the JMX role for the JMX service for the Cassandra instance
     */
    @Override
    @JsonProperty("jmx_role")
    public String jmxRole()
    {
        return jmxRole;
    }

    /**
     * @return the password for the JMX role for the JMX service for the Cassandra instance
     */
    @Override
    @JsonProperty("jmx_role_password")
    public String jmxRolePassword()
    {
        return jmxRolePassword;
    }

    public static Builder<?> builder()
    {
        return new Builder<>();
    }

    /**
     * {@code InstanceConfigurationImpl} builder static inner class.
     *
     * @param <T> the builder type
     */
    public static class Builder<T extends Builder<?>> implements DataObjectBuilder<T, InstanceConfigurationImpl>
    {
        protected int id;
        protected String host;
        protected int port;
        protected String username;
        protected String password;
        protected List<String> dataDirs;
        protected String stagingDir;
        protected String jmxHost;
        protected int jmxPort;
        protected boolean jmxSslEnabled;
        protected String jmxRole;
        protected String jmxRolePassword;

        protected Builder()
        {
        }

        /**
         * Sets the {@code id} and returns a reference to this Builder enabling method chaining.
         *
         * @param id the {@code id} to set
         * @return a reference to this Builder
         */
        public T id(int id)
        {
            return update(b -> b.id = id);
        }

        /**
         * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
         *
         * @param host the {@code host} to set
         * @return a reference to this Builder
         */
        public T host(String host)
        {
            return update(b -> b.host = host);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public T port(int port)
        {
            return update(b -> b.port = port);
        }

        /**
         * Sets the {@code username} and returns a reference to this Builder enabling method chaining.
         *
         * @param username the {@code username} to set
         * @return a reference to this Builder
         */
        public T username(String username)
        {
            return update(b -> b.username = username);
        }

        /**
         * Sets the {@code password} and returns a reference to this Builder enabling method chaining.
         *
         * @param password the {@code password} to set
         * @return a reference to this Builder
         */
        public T password(String password)
        {
            return update(b -> b.password = password);
        }

        /**
         * Sets the {@code dataDirs} and returns a reference to this Builder enabling method chaining.
         *
         * @param dataDirs the {@code dataDirs} to set
         * @return a reference to this Builder
         */
        public T dataDirs(String... dataDirs)
        {
            return update(b -> b.dataDirs = Arrays.asList(dataDirs));
        }

        /**
         * Sets the {@code dataDirs} and returns a reference to this Builder enabling method chaining.
         *
         * @param dataDirs the {@code dataDirs} to set
         * @return a reference to this Builder
         */
        public T dataDirs(List<String> dataDirs)
        {
            return update(b -> b.dataDirs = dataDirs);
        }

        /**
         * Sets the {@code stagingDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param stagingDir the {@code stagingDir} to set
         * @return a reference to this Builder
         */
        public T stagingDir(String stagingDir)
        {
            return update(b -> b.stagingDir = stagingDir);
        }

        /**
         * Sets the {@code jmxHost} and returns a reference to this Builder enabling method chaining.
         *
         * @param jmxHost the {@code jmxHost} to set
         * @return a reference to this Builder
         */
        public T jmxHost(String jmxHost)
        {
            return update(b -> b.jmxHost = jmxHost);
        }

        /**
         * Sets the {@code jmxPort} and returns a reference to this Builder enabling method chaining.
         *
         * @param jmxPort the {@code jmxPort} to set
         * @return a reference to this Builder
         */
        public T jmxPort(int jmxPort)
        {
            return update(b -> b.jmxPort = jmxPort);
        }

        /**
         * Sets the {@code jmxSslEnabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param jmxSslEnabled the {@code jmxSslEnabled} to set
         * @return a reference to this Builder
         */
        public T jmxSslEnabled(boolean jmxSslEnabled)
        {
            return update(b -> b.jmxSslEnabled = jmxSslEnabled);
        }

        /**
         * Sets the {@code jmxRole} and returns a reference to this Builder enabling method chaining.
         *
         * @param jmxRole the {@code jmxRole} to set
         * @return a reference to this Builder
         */
        public T jmxRole(String jmxRole)
        {
            return update(b -> b.jmxRole = jmxRole);
        }

        /**
         * Sets the {@code jmxRolePassword} and returns a reference to this Builder enabling method chaining.
         *
         * @param jmxRolePassword the {@code jmxRolePassword} to set
         * @return a reference to this Builder
         */
        public T jmxRolePassword(String jmxRolePassword)
        {
            return update(b -> b.jmxRolePassword = jmxRolePassword);
        }

        /**
         * Returns a {@code InstanceConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code InstanceConfigurationImpl} built with parameters of this
         * {@code InstanceConfigurationImpl.Builder}
         */
        @Override
        public InstanceConfigurationImpl build()
        {
            return new InstanceConfigurationImpl(this);
        }
    }
}
