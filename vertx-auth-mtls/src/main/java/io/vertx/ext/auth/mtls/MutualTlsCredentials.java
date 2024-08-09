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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.authentication.Credentials;

/**
 * A class representing MutualTLS Credentials. This is used to pass details of a user
 * and eventually get authenticated.
 */
public class MutualTlsCredentials implements Credentials
{
    private final List<Certificate> certificateChain;

    @VisibleForTesting
    public MutualTlsCredentials(List<Certificate> certificateChain)
    {
        this.certificateChain = certificateChain;
    }

    public MutualTlsCredentials(HttpServerRequest req)
    {
        this(extractCertificateChain(req));
    }

    /**
     * Checks whether the Mutual TLS Credential object that an instance is representing is valid.
     * This does so by checking that the certificate chain is a non-null, non-empty list of
     * certificates.
     */
    @Override
    public <V> void checkValid(V arg) throws CredentialValidationException
    {
        if (certificateChain == null || certificateChain.isEmpty())
        {
            throw new CredentialValidationException("Certificate Chain cannot be null or empty");
        }
    }

    /**
     * Checks whether the Mutual TLS Credential object that an instance is representing is valid.
     * This does so by checking that the certificate chain is a non-null, non-empty list of
     * certificates.
     */
    public void validate() throws CredentialValidationException
    {
        if (certificateChain == null || certificateChain.isEmpty())
        {
            throw new CredentialValidationException("Certificate Chain cannot be null or empty");
        }
    }

    /**
     * <i>Deprecated</i>
     * {@inheritDoc}
     */
    @Override
    public JsonObject toJson()
    {
        throw new UnsupportedOperationException("Deprecated authentication method");
    }

    /**
     * Extracts the certificate chain from the {@code RoutingContext}.
     *
     * @param req - {@code HttpServerRequest} representing the clients request
     * @return The certificate chain as a list of certificates
     */
    private static List<Certificate> extractCertificateChain(HttpServerRequest req)
    {
        List<Certificate> certificateChain;
        try
        {
            certificateChain = Collections.unmodifiableList(req.connection().peerCertificates());
        }
        catch (Exception e)
        {
            certificateChain = new ArrayList<>();
        }
        return certificateChain;
    }

    /**
     * Returns the certificate chain of the credentials that a particular credential is
     * representing.
     *
     * @return The certificate chain as a list of certificates
     */
    public List<Certificate> certificateChain()
    {
        return certificateChain;
    }
}
