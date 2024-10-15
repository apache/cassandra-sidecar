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

package io.vertx.ext.auth.authentication;

import java.security.cert.Certificate;
import java.util.Collections;
import java.util.List;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

/**
 * Certificates based {@link Credentials} implementation, carries user's certificates, which can be used for
 * authenticating or authorizing users.
 */
public class CertificateCredentials implements Credentials
{
    private final List<Certificate> certificateChain;

    public CertificateCredentials(List<Certificate> certificateChain)
    {
        this.certificateChain = Collections.unmodifiableList(certificateChain);
    }

    /**
     * @return The certificate chain contained in {@link CertificateCredentials}
     */
    public List<Certificate> certificateChain()
    {
        return certificateChain;
    }

    public void checkValid() throws CredentialValidationException
    {
        checkValid(this);
    }

    @Override
    public <V> void checkValid(V arg) throws CredentialValidationException
    {
        if (certificateChain.isEmpty())
        {
            throw new CredentialValidationException("Certificate Chain cannot be empty");
        }
    }

    /**
     * @deprecated {@link CertificateCredentials} currently not represented in Json format.
     */
    @Override
    public JsonObject toJson()
    {
        throw new UnsupportedOperationException("Deprecated authentication method");
    }

    public static CertificateCredentials fromRequest(HttpServerRequest request)
    {
        return new CertificateCredentials(extractCertificateChain(request));
    }

    /**
     * @return The certificate chain as a list of certificates
     */
    private static List<Certificate> extractCertificateChain(HttpServerRequest request)
    {
        try
        {
            return request.connection().peerCertificates();
        }
        catch (Exception e)
        {
            throw new InvalidCredentialException("Could not extract certificates from request", e);
        }
    }
}
