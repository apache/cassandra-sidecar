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

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates the basic configuration needed to connect to a single Cassandra instance
 */
public class InstanceConfigurationImpl implements InstanceConfiguration
{
    protected final int id;
    protected final String host;
    protected final int port;
    protected final String storageDir;
    protected final List<String> dataDirs;
    protected final String stagingDir;
    protected final String cdcDir;
    protected final String commitlogDir;
    protected final String hintsDir;
    protected final String savedCachesDir;
    protected final String localSystemDataFileDir;
    protected final String jmxHost;
    protected final int jmxPort;
    protected final boolean jmxSslEnabled;
    protected final String jmxRole;
    protected final String jmxRolePassword;

    @JsonCreator
    public InstanceConfigurationImpl(@JsonProperty("id") int id,
                                     @NotNull @JsonProperty("host") String host,
                                     @JsonProperty("port") int port,
                                     @Nullable @JsonProperty("storage_dir") String storageDir,
                                     @Nullable @JsonProperty("data_dirs") List<String> dataDirs,
                                     @NotNull @JsonProperty("staging_dir") String stagingDir,
                                     @Nullable @JsonProperty("cdc_dir") String cdcDir,
                                     @Nullable @JsonProperty("commitlog_dir") String commitlogDir,
                                     @Nullable @JsonProperty("hints_dir") String hintsDir,
                                     @Nullable @JsonProperty("saved_caches_dir") String savedCachesDir,
                                     @Nullable @JsonProperty("local_system_data_file_dir") String localSystemDataFileDir,
                                     @NotNull @JsonProperty("jmx_host") String jmxHost,
                                     @JsonProperty("jmx_port") int jmxPort,
                                     @JsonProperty("jmx_ssl_enabled") boolean jmxSslEnabled,
                                     @Nullable @JsonProperty("jmx_role") String jmxRole,
                                     @Nullable @JsonProperty("jmx_role_password") String jmxRolePassword)
    {
        this.id = id;
        this.host = host;
        this.port = port;
        this.storageDir = storageDir;
        this.dataDirs = dataDirs != null ? Collections.unmodifiableList(dataDirs) : null;
        this.stagingDir = stagingDir;
        this.cdcDir = cdcDir;
        this.commitlogDir = commitlogDir;
        this.hintsDir = hintsDir;
        this.savedCachesDir = savedCachesDir;
        this.localSystemDataFileDir = localSystemDataFileDir;
        this.jmxHost = jmxHost;
        this.jmxPort = jmxPort;
        this.jmxSslEnabled = jmxSslEnabled;
        this.jmxRole = jmxRole;
        this.jmxRolePassword = jmxRolePassword;
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

    @Override
    @JsonProperty("storage_dir")
    public String storageDir()
    {
        return storageDir;
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
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("commitlog_dir")
    public String commitlogDir()
    {
        return commitlogDir;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("hints_dir")
    public String hintsDir()
    {
        return hintsDir;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("saved_caches_dir")
    public String savedCachesDir()
    {
        return savedCachesDir;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("local_system_data_file_dir")
    public String localSystemDataFileDir()
    {
        return localSystemDataFileDir;
    }

    /**
     * @return cdc directory of the cassandra instance
     */
    @Override
    @JsonProperty("cdc_dir")
    public String cdcDir()
    {
        return cdcDir;
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
}
