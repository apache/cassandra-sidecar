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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.mtls.CertificateIdentityExtractor;
import io.vertx.ext.auth.mtls.CertificateValidator;

/**
 * {@link AuthenticationProvider} implementation for mTLS (MutualTLS) authentication. With mTLS authentication
 * both server and client exchange certificates and validates each other's certificates.
 */
public class MutualTlsAuthenticationProvider implements AuthenticationProvider
{
    private final CertificateValidator certificateValidator;
    private final CertificateIdentityExtractor identityExtractor;

    public MutualTlsAuthenticationProvider(CertificateValidator certificateValidator,
                                           CertificateIdentityExtractor identityExtractor)
    {
        this.certificateValidator = certificateValidator;
        this.identityExtractor = identityExtractor;
    }

    @Override
    public Future<User> authenticate(Credentials credentials)
    {
        if (!(credentials instanceof CertificateCredentials))
        {
            return Future.failedFuture("CertificateCredentials expected for mTLS authentication");
        }

        CertificateCredentials certificateCredentials = (CertificateCredentials) credentials;
        try
        {
            certificateValidator.verifyCertificate(certificateCredentials);
            String identity = identityExtractor.validIdentity(certificateCredentials);
            return Future.succeededFuture(User.fromName(identity));
        }
        catch (Exception e)
        {
            return Future.failedFuture(e);
        }
    }

    /**
     * Use {@code authenticate(Credentials credentials, Handler<AsyncResult<User>> resultHandler)} instead
     */
    @Deprecated
    @Override
    public void authenticate(JsonObject credentials, Handler<AsyncResult<User>> resultHandler)
    {
        throw new UnsupportedOperationException("Deprecated authentication method");
    }
}
