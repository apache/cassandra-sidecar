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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.mtls.CertificateIdentityExtractor;
import io.vertx.ext.auth.mtls.CertificateValidator;
import io.vertx.ext.auth.mtls.MutualTlsAuthentication;
import io.vertx.ext.auth.mtls.impl.MutualTlsAuthenticationImpl;
import io.vertx.ext.auth.mtls.impl.SpiffeIdentityExtractor;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerInternal;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * {@link AuthenticationHandlerFactory} implementation for {@link MutualTlsAuthenticationHandler}
 */
@Singleton
public class MutualTlsAuthenticationHandlerFactory implements AuthenticationHandlerFactory
{
    protected static final String CERTIFICATE_VALIDATOR_PARAM_KEY = "certificate_validator";
    protected static final String CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY = "certificate_identity_extractor";
    private final IdentityToRoleCache identityToRoleCache;

    @Inject
    public MutualTlsAuthenticationHandlerFactory(IdentityToRoleCache identityToRoleCache)
    {
        this.identityToRoleCache = identityToRoleCache;
    }

    @Override
    public AuthenticationHandlerInternal create(Vertx vertx,
                                                AccessControlConfiguration accessControlConfiguration,
                                                Map<String, String> parameters) throws ConfigurationException
    {
        validate(parameters);
        try
        {
            return createInternal(vertx, accessControlConfiguration, parameters);
        }
        catch (Exception exception)
        {
            throw new ConfigurationException("Error creating MutualTlsAuthenticationHandler", exception);
        }
    }

    void validate(Map<String, String> parameters) throws ConfigurationException
    {
        if (parameters == null)
        {
            throw new ConfigurationException("Parameters cannot be null for MutualTlsAuthenticationHandlerFactory");
        }

        if (!parameters.containsKey(CERTIFICATE_VALIDATOR_PARAM_KEY))
        {
            throw new ConfigurationException(String.format("Missing %s parameter for MutualTlsAuthenticationHandler creation", CERTIFICATE_VALIDATOR_PARAM_KEY));
        }

        if (!parameters.containsKey(CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY))
        {
            throw new ConfigurationException(String.format("Missing %s parameter for MutualTlsAuthenticationHandler creation", CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY));
        }
    }

    private MutualTlsAuthenticationHandler createInternal(Vertx vertx,
                                                          AccessControlConfiguration accessControlConfiguration,
                                                          Map<String, String> parameters) throws Exception
    {
        CertificateValidator certificateValidator = (CertificateValidator) Class.forName(parameters.get(CERTIFICATE_VALIDATOR_PARAM_KEY)).newInstance();
        CertificateIdentityExtractor certificateIdentityExtractor;
        if (parameters.get(CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY).equalsIgnoreCase(CassandraIdentityExtractor.class.getName()))
        {
            certificateIdentityExtractor = new CassandraIdentityExtractor(identityToRoleCache,
                                                                          accessControlConfiguration.adminIdentities());
        }
        else
        {
            certificateIdentityExtractor = new SpiffeIdentityExtractor();
        }
        MutualTlsAuthentication mTLSAuthProvider = new MutualTlsAuthenticationImpl(vertx, certificateValidator, certificateIdentityExtractor);
        return new MutualTlsAuthenticationHandler(mTLSAuthProvider);
    }
}
