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

package io.vertx.ext.auth.mtls;

import java.security.cert.Certificate;
import java.util.List;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.mtls.exceptions.AuthenticationException;
import org.apache.http.HttpException;

/**
 * Authentication provider for MutualTLS. Allows for authentication of users through the use of certificates.
 */
public class MutualTlsAuthenticationProvider implements AuthenticationProvider
{
    private final MutualTlsCertificateValidator certificateValidator;
    private final MutualTlsIdentityValidator identityValidator;

    public MutualTlsAuthenticationProvider(MutualTlsCertificateValidator certificateValidator, MutualTlsIdentityValidator identityValidator)
    {
        this.certificateValidator = certificateValidator;
        this.identityValidator = identityValidator;
    }

    /**
     * Authenticates a user with mTLS.
     *
     * @param credentials The credentials
     * @return {@code Future<User>} representing the status of authentication
     */
    @Override
    public Future<User> authenticate(Credentials credentials)
    {
        try
        {
            return Future.succeededFuture(authenticateInternal(credentials));
        }
        catch (Throwable throwable)
        {
            return Future.failedFuture(new CredentialValidationException("Unable to authenticate", throwable));
        }
    }

    private User authenticateInternal(Credentials credentials) throws HttpException
    {
        if (!certificateValidator.isValidCertificate(credentials))
        {
            String msg = "Invalid or not supported certificate";
            throw (new HttpException(msg));
        }

        List<Certificate> clientCertificateChain = ((MutualTlsCredentials) credentials).certificateChain();
        Certificate[] clientCertificateChainArr = clientCertificateChain.toArray(new Certificate[0]);

        String identity;
        try
        {
            identity = certificateValidator.identity(clientCertificateChainArr);
        }
        catch (AuthenticationException e)
        {
            String msg = "Unable to extract client identity from certificate for authentication";
            throw (new HttpException(msg));
        }
        if (identity == null || identity.isEmpty())
        {
            String msg = "Unable to extract client identity from certificate for authentication";
            throw (new HttpException(msg));
        }

        if (!identityValidator.isValidIdentity(identity))
        {
            String msg = "Client identity not authenticated";
            throw (new HttpException(msg));
        }

        return identityValidator.userFromIdentity(identity);
    }

    @Override
    public void authenticate(JsonObject credentials, Handler<AsyncResult<User>> resultHandler)
    {
        throw new UnsupportedOperationException("Deprecated authentication method");
    }
}
