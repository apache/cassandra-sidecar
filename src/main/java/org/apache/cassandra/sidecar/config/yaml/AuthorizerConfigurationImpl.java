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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.auth.authorization.AuthorizerConfig;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.AuthorizerConfiguration;
import org.apache.cassandra.sidecar.config.RoleToSidecarPermissionsConfiguration;

/**
 * {@inheritDoc}
 */
public class AuthorizerConfigurationImpl implements AuthorizerConfiguration
{
    @JsonProperty(value = "auth_config")
    protected AuthorizerConfig authConfig;

    @JsonProperty(value = "role_to_sidecar_permissions")
    protected List<RoleToSidecarPermissionsConfiguration> roleToSidecarPermissions;

    public AuthorizerConfigurationImpl()
    {
        this(new Builder());
    }

    protected AuthorizerConfigurationImpl(Builder builder)
    {
        authConfig = builder.authConfig;
        roleToSidecarPermissions = builder.roleToSidecarPermissions;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "auth_config")
    public AuthorizerConfig authConfig()
    {
        return authConfig;
    }

    @Override
    @JsonProperty(value = "role_to_sidecar_permissions")
    public List<RoleToSidecarPermissionsConfiguration> roleToSidecarPermissions()
    {
        return roleToSidecarPermissions;
    }

    /**
     * {@code AuthenticatorConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, AuthorizerConfiguration>
    {
        protected AuthorizerConfig authConfig = DEFAULT_AUTHORIZER;
        protected List<RoleToSidecarPermissionsConfiguration> roleToSidecarPermissions;

        protected Builder()
        {
        }

        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code authConfig} and returns a reference to this Builder enabling method chaining.
         *
         * @param authConfig the {@code String} representation of the desired authentication scheme to set
         * @return a reference to this Builder
         */
        public Builder authConfig(AuthorizerConfig authConfig)
        {
            return update(b -> b.authConfig = authConfig);
        }

        public Builder roleToSidecarPermissions(List<RoleToSidecarPermissionsConfiguration> roleToSidecarPermissions)
        {
            return update(b -> b.roleToSidecarPermissions = roleToSidecarPermissions);
        }

        /**
         * Build into data object of type R
         *
         * @return data object type
         */
        public AuthorizerConfiguration build()
        {
            return new AuthorizerConfigurationImpl(this);
        }
    }
}
