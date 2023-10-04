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
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;

/**
 * Encapsulates key or trust store option configurations
 */
public class KeyStoreConfigurationImpl implements KeyStoreConfiguration
{
    @JsonProperty("path")
    protected final String path;

    @JsonProperty("password")
    protected final String password;

    @JsonProperty(value = "type", defaultValue = DEFAULT_TYPE)
    protected final String type;

    @JsonProperty(value = "check_interval_sec", defaultValue = DEFAULT_CHECK_INTERVAL_SECONDS + "")
    protected final int checkIntervalInSeconds;

    public KeyStoreConfigurationImpl()
    {
        this(null, null, DEFAULT_TYPE, DEFAULT_CHECK_INTERVAL_SECONDS);
    }

    public KeyStoreConfigurationImpl(String path, String password)
    {
        this(path, password, DEFAULT_TYPE, DEFAULT_CHECK_INTERVAL_SECONDS);
    }

    public KeyStoreConfigurationImpl(String path, String password, String type, int checkIntervalInSeconds)
    {
        this.path = path;
        this.password = password;
        this.type = type;
        this.checkIntervalInSeconds = checkIntervalInSeconds;
    }

    /**
     * @return the path to the store
     */
    @Override
    @JsonProperty("path")
    public String path()
    {
        return path;
    }

    /**
     * @return the password for the store
     */
    @Override
    @JsonProperty("password")
    public String password()
    {
        return password;
    }

    /**
     * @return the type of the store
     */
    @Override
    @JsonProperty(value = "type", defaultValue = DEFAULT_TYPE)
    public String type()
    {
        return type;
    }

    /**
     * @return the interval, in seconds, in which the key store will be checked for changes in the filesystem
     */
    @Override
    @JsonProperty(value = "check_interval_sec", defaultValue = DEFAULT_CHECK_INTERVAL_SECONDS + "")
    public int checkIntervalInSeconds()
    {
        return checkIntervalInSeconds;
    }
}
