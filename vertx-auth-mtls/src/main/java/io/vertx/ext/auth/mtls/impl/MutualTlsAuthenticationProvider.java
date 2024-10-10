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

package io.vertx.ext.auth.mtls.impl;

import java.security.cert.Certificate;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.mtls.CertificateIdentityExtractor;
import io.vertx.ext.auth.mtls.CertificateIdentityValidator;
import io.vertx.ext.auth.mtls.CertificateValidator;

/**
 * {@link AuthenticationProvider} implementation for mTLS (MutualTLS) authentication. With mTLS authentication
 * both server and client exchange certificates and validates each other's certificates.
 */
public class MutualTlsAuthenticationProvider implements AuthenticationProvider
{
    private final CertificateValidator certificateValidator;
    private final CertificateIdentityExtractor identityExtractor;
    private final CertificateIdentityValidator identityValidator;

    public MutualTlsAuthenticationProvider(CertificateValidator certificateValidator,
                                           CertificateIdentityExtractor identityExtractor,
                                           CertificateIdentityValidator identityValidator)
    {
        this.certificateValidator = certificateValidator;
        this.identityExtractor = identityExtractor;
        this.identityValidator = identityValidator;
    }

    @Override
    public Future<User> authenticate(Credentials credentials)
    {
        if (!(credentials instanceof CertificateCredentials))
        {
            return Future.failedFuture("CertificateCredentials expected for mTLS authentication");
        }

        CertificateCredentials certificateCredentials = (CertificateCredentials) credentials;
        String identity;
        try
        {
            certificateValidator.isValidCertificate(certificateCredentials);
            identity = identityExtractor.identity(certificateCredentials.certificateChain().toArray(new Certificate[0]));
        }
        catch (Exception e)
        {
            return Future.failedFuture("Invalid certificate passed");
        }

        if (identity == null || identity.isEmpty())
        {
            return Future.failedFuture("Could not extract identity from certificate");
        }
        if (!identityValidator.isValidIdentity(identity))
        {
            return Future.failedFuture("Certificate identity is not a valid identity");
        }
        return Future.succeededFuture(User.fromName(identity));
    }

    /**
     * @deprecated use {@code authenticate(Credentials credentials, Handler<AsyncResult<User>> resultHandler)} instead
     */
    @Override
    public void authenticate(JsonObject credentials, Handler<AsyncResult<User>> resultHandler)
    {
        throw new UnsupportedOperationException("Deprecated authentication method");
    }
}
