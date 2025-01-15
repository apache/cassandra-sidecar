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
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;

/**
 * Encapsulates key or trust store option configurations
 */
public class KeyStoreConfigurationImpl implements KeyStoreConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyStoreConfigurationImpl.class);

    @JsonProperty("path")
    protected final String path;

    @JsonProperty("password")
    protected final String password;

    @JsonProperty(value = "type")
    protected final String type;

    protected SecondBoundConfiguration checkInterval;

    public KeyStoreConfigurationImpl()
    {
        this(null, null, DEFAULT_TYPE, SecondBoundConfiguration.ZERO);
    }

    public KeyStoreConfigurationImpl(String path, String password)
    {
        this(path, password, DEFAULT_TYPE, SecondBoundConfiguration.ZERO);
    }

    public KeyStoreConfigurationImpl(String path, String password, String type)
    {
        this(path, password, type, SecondBoundConfiguration.ZERO);
    }

    public KeyStoreConfigurationImpl(String path, String password, String type, SecondBoundConfiguration checkInterval)
    {
        this.path = path;
        this.password = password;
        this.type = type;
        this.checkInterval = checkInterval;
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
    @JsonProperty(value = "type")
    public String type()
    {
        return type;
    }

    /**
     * @return the interval in which the key store will be checked for changes in the filesystem
     */
    @Override
    @JsonProperty(value = "check_interval")
    public SecondBoundConfiguration checkInterval()
    {
        return checkInterval;
    }

    @JsonProperty(value = "check_interval")
    public void setCheckInterval(SecondBoundConfiguration checkInterval)
    {
        this.checkInterval = checkInterval;
    }

    /**
     * Legacy property {@code check_interval_sec}
     *
     * @param checkIntervalInSeconds interval in milliseconds
     * @deprecated in favor of {@code check_interval}
     */
    @JsonProperty(value = "check_interval_sec")
    @Deprecated
    public void setCheckIntervalInSeconds(long checkIntervalInSeconds)
    {
        LOGGER.warn("'check_interval_sec' is deprecated, use 'check_interval' instead");
        setCheckInterval(new SecondBoundConfiguration(checkIntervalInSeconds, TimeUnit.SECONDS));
    }
}
