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
import io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl;
import io.vertx.ext.auth.mtls.impl.SpiffeIdentityExtractor;
import org.apache.cassandra.sidecar.config.MutualTlsAuthenticatorConfiguration;

/**
 * {@inheritDoc}
 */
public class MutualTlsAuthenticatorConfigurationImpl implements MutualTlsAuthenticatorConfiguration
{
    private static final boolean DEFAULT_ENABLED = false;
    private static final String DEFAULT_CERTIFICATE_VALIDATOR = CertificateValidatorImpl.class.getCanonicalName();
    private static final String DEFAULT_CERTIFICATE_IDENTITY_EXTRACTOR = SpiffeIdentityExtractor.class.getCanonicalName();

    @JsonProperty(value = "enabled")
    protected final boolean enabled;

    @JsonProperty(value = "certificate_validator")
    protected final String certificateValidator;

    @JsonProperty(value = "certificate_identity_extractor")
    protected final String identityExtractor;

    public MutualTlsAuthenticatorConfigurationImpl()
    {
        this(DEFAULT_ENABLED, DEFAULT_CERTIFICATE_VALIDATOR, DEFAULT_CERTIFICATE_IDENTITY_EXTRACTOR);
    }

    public MutualTlsAuthenticatorConfigurationImpl(boolean enabled, String certificateValidator, String identityExtractor)
    {
        this.enabled = enabled;
        this.certificateValidator = certificateValidator;
        this.identityExtractor = identityExtractor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "enabled")
    public boolean enabled()
    {
        return enabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "certificate_validator")
    public String certificateValidator()
    {
        return certificateValidator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "certificate_identity_extractor")
    public String certificateIdentityExtractor()
    {
        return identityExtractor;
    }
}
