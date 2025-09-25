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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.acl.authorization.AllowAllAuthorizationProvider;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
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
    private static final ParameterizedClassConfiguration DEFAULT_AUTHORIZER_CONFIGURATION
    = new ParameterizedClassConfigurationImpl(AllowAllAuthorizationProvider.class.getName(), Collections.emptyMap());
    private static final Set<String> DEFAULT_ADMIN_IDENTITIES = Collections.emptySet();
    private static final CacheConfiguration DEFAULT_PERMISSION_CACHE_CONFIGURATION
    = CacheConfigurationImpl.builder()
                            .expireAfterAccess(MillisecondBoundConfiguration.parse("2h"))
                            .maximumSize(1_000)
                            .build();

    @JsonProperty(value = "enabled")
    protected final boolean enabled;

    @JsonProperty(value = "authenticators")
    protected final List<ParameterizedClassConfiguration> authenticatorsConfiguration;

    @JsonProperty(value = "authorizer")
    protected final ParameterizedClassConfiguration authorizerConfiguration;

    @JsonProperty(value = "admin_identities")
    protected final Set<String> adminIdentities;

    @JsonProperty(value = "permission_cache")
    protected final CacheConfiguration permissionCacheConfiguration;

    public AccessControlConfigurationImpl()
    {
        this(builder());
    }

    protected AccessControlConfigurationImpl(Builder builder)
    {
        enabled = builder.enabled;
        authenticatorsConfiguration = builder.authenticatorsConfiguration;
        authorizerConfiguration = builder.authorizerConfiguration;
        adminIdentities = builder.adminIdentities;
        permissionCacheConfiguration = builder.permissionCacheConfiguration;
    }

    /**
     * {@inheritDoc}
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
    @JsonProperty(value = "authorizer")
    public ParameterizedClassConfiguration authorizerConfiguration()
    {
        return authorizerConfiguration;
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

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code AccessControlConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, AccessControlConfigurationImpl>
    {
        private boolean enabled = DEFAULT_ENABLED;
        private List<ParameterizedClassConfiguration> authenticatorsConfiguration = DEFAULT_AUTHENTICATORS_CONFIGURATION;
        private ParameterizedClassConfiguration authorizerConfiguration = DEFAULT_AUTHORIZER_CONFIGURATION;
        private Set<String> adminIdentities = DEFAULT_ADMIN_IDENTITIES;
        private CacheConfiguration permissionCacheConfiguration = DEFAULT_PERMISSION_CACHE_CONFIGURATION;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code enabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param enabled the {@code enabled} to set
         * @return a reference to this Builder
         */
        public Builder enabled(boolean enabled)
        {
            return update(b -> b.enabled = enabled);
        }

        /**
         * Sets the {@code authenticatorsConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param authenticatorsConfiguration the {@code authenticatorsConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder authenticatorsConfiguration(List<ParameterizedClassConfiguration> authenticatorsConfiguration)
        {
            return update(b -> b.authenticatorsConfiguration = authenticatorsConfiguration);
        }

        /**
         * Sets the {@code authorizerConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param authorizerConfiguration the {@code authorizerConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder authorizerConfiguration(ParameterizedClassConfiguration authorizerConfiguration)
        {
            return update(b -> b.authorizerConfiguration = authorizerConfiguration);
        }

        /**
         * Sets the {@code adminIdentities} and returns a reference to this Builder enabling method chaining.
         *
         * @param adminIdentities the {@code adminIdentities} to set
         * @return a reference to this Builder
         */
        public Builder adminIdentities(Set<String> adminIdentities)
        {
            return update(b -> b.adminIdentities = adminIdentities);
        }

        /**
         * Sets the {@code permissionCacheConfiguration} and returns a reference to this Builder enabling method chaining.
         *
         * @param permissionCacheConfiguration the {@code permissionCacheConfiguration} to set
         * @return a reference to this Builder
         */
        public Builder permissionCacheConfiguration(CacheConfiguration permissionCacheConfiguration)
        {
            return update(b -> b.permissionCacheConfiguration = permissionCacheConfiguration);
        }

        /**
         * Returns a {@code AccessControlConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code AccessControlConfigurationImpl} built with parameters of this {@code AccessControlConfigurationImpl.Builder}
         */
        @Override
        public AccessControlConfigurationImpl build()
        {
            return new AccessControlConfigurationImpl(this);
        }
    }
}
