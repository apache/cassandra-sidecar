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

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.auth.authentication.AuthenticatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.CertificateValidatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.IdentityValidatorConfig;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.AuthenticatorConfiguration;

/**
 * Encapsulates Authentication Configuration
 */
public class AuthenticatorConfigurationImpl implements AuthenticatorConfiguration
{
    @JsonProperty(value = "authorized_identities")
    protected final Set<String> authorizedIdentities;

    @JsonProperty(value = "auth_config")
    protected AuthenticatorConfig authConfig;

    @JsonProperty(value = "cert_validator")
    protected CertificateValidatorConfig certValidator;

    @JsonProperty(value = "id_validator")
    protected IdentityValidatorConfig idValidator;

    public AuthenticatorConfigurationImpl()
    {
        this(new Builder());
    }

    protected AuthenticatorConfigurationImpl(Builder builder)
    {
        authorizedIdentities = builder.authorizedIdentities;
        authConfig = builder.authConfig;
        certValidator = builder.certValidator;
        idValidator = builder.idValidator;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "authorized_identities")
    public Set<String> authorizedIdentities()
    {
        return authorizedIdentities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "auth_config")
    public AuthenticatorConfig authConfig()
    {
        return authConfig;
    }

    /**
     * Returns the desired certificate validator as provided in the yaml file
     *
     * @return A {@code String} representation of desired certificate validator
     */
    public CertificateValidatorConfig certValidator()
    {
        return certValidator;
    }

    /**
     * Returns the desired identity validator as provided in the yaml file
     *
     * @return A {@code String} representation of desired identity validator
     */
    public IdentityValidatorConfig idValidator()
    {
        return idValidator;
    }


    /**
     * {@code AuthenticatorConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, AuthenticatorConfiguration>
    {
        protected AuthenticatorConfig authConfig = DEFAULT_AUTHENTICATOR;
        protected Set<String> authorizedIdentities;
        protected CertificateValidatorConfig certValidator = DEFAULT_CERT_VALIDATOR;
        protected IdentityValidatorConfig idValidator = DEFAULT_ID_VALIDATOR;

        protected Builder()
        {
        }

        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code authorizedIdentities} and returns a reference to this Builder enabling method chaining.
         *
         * @param authorizedIdentities the {@code Set<String>} of identities to set
         * @return a reference to this Builder
         */
        public Builder authorizedIdentities(Set<String> authorizedIdentities)
        {
            return update(b -> b.authorizedIdentities = authorizedIdentities);
        }

        /**
         * Sets the {@code authConfig} and returns a reference to this Builder enabling method chaining.
         *
         * @param authConfig the {@code String} representation of the desired authentication scheme to set
         * @return a reference to this Builder
         */
        public Builder authConfig(AuthenticatorConfig authConfig)
        {
            return update(b -> b.authConfig = authConfig);
        }

        /**
         * Sets the {@code certValidator} and returns a reference to this Builder enabling method chaining.
         *
         * @param certValidator the {@code String} representation of the desired certificate validator to set
         * @return a reference to this Builder
         */
        public Builder certValidator(CertificateValidatorConfig certValidator)
        {
            return update(b -> b.certValidator = certValidator);
        }

        public Builder idValidator(IdentityValidatorConfig idValidator)
        {
            return update(b -> b.idValidator = idValidator);
        }

        /**
         * Build into data object of type R
         *
         * @return data object type
         */
        public AuthenticatorConfiguration build()
        {
            return new AuthenticatorConfigurationImpl(this);
        }
    }
}
