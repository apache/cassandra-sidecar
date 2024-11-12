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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;

/**
 * {@inheritDoc}
 */
public class AccessControlConfigurationImpl implements AccessControlConfiguration
{
    private static final boolean DEFAULT_ENABLED = false;
    private static final List<ParameterizedClassConfiguration> DEFAULT_AUTHENTICATORS_CONFIGURATION = Collections.emptyList();
    private static final Set<String> DEFAULT_ADMIN_IDENTITIES = Collections.emptySet();
    private static final CacheConfiguration DEFAULT_PERMISSION_CACHE_CONFIGURATION = new CacheConfigurationImpl(TimeUnit.HOURS.toMillis(2), 1_000);

    @JsonProperty(value = "enabled")
    protected final boolean enabled;

    @JsonProperty(value = "authenticators")
    protected final List<ParameterizedClassConfiguration> authenticatorsConfiguration;

    @JsonProperty(value = "admin_identities")
    protected final Set<String> adminIdentities;

    @JsonProperty(value = "permission_cache")
    protected final CacheConfiguration permissionCacheConfiguration;

    public AccessControlConfigurationImpl()
    {
        this(DEFAULT_ENABLED, DEFAULT_AUTHENTICATORS_CONFIGURATION, DEFAULT_ADMIN_IDENTITIES, DEFAULT_PERMISSION_CACHE_CONFIGURATION);
    }

    public AccessControlConfigurationImpl(boolean enabled,
                                          List<ParameterizedClassConfiguration> authenticatorsConfiguration,
                                          Set<String> adminIdentities,
                                          CacheConfiguration permissionCacheConfiguration)
    {
        this.enabled = enabled;
        this.authenticatorsConfiguration = authenticatorsConfiguration;
        this.adminIdentities = adminIdentities;
        this.permissionCacheConfiguration = permissionCacheConfiguration;
    }

    /**
     * @inheritDoc
     */
    @Override
    @JsonProperty
    public boolean enabled()
    {
        return enabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "authenticators")
    public List<ParameterizedClassConfiguration> authenticatorsConfiguration()
    {
        return authenticatorsConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "admin_identities")
    public Set<String> adminIdentities()
    {
        return adminIdentities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "permission_cache")
    public CacheConfiguration permissionCacheConfiguration()
    {
        return permissionCacheConfiguration;
    }
}
