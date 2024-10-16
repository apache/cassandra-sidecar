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
import java.security.cert.X509Certificate;
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
    private final X509Certificate peerCertificate;

    public CertificateCredentials(Certificate certificate)
    {
        this(Collections.singletonList(certificate));
    }

    public CertificateCredentials(List<Certificate> certificateChain)
    {
        this.certificateChain = Collections.unmodifiableList(certificateChain);
        this.peerCertificate = getPeerCertificate();
    }

    /**
     * Create {@link CertificateCredentials} from {@link HttpServerRequest}
     *
     * @return CertificateCredentials
     */
    public static CertificateCredentials fromHttpRequest(HttpServerRequest request)
    {
        try
        {
            return new CertificateCredentials(request.connection().peerCertificates());
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Could not extract certificates from request", e);
        }
    }

    /**
     * @return The certificate chain contained in {@link CertificateCredentials}
     */
    public List<Certificate> certificateChain()
    {
        return certificateChain;
    }

    /**
     * @return peer's certificate. It does not return null value once {@link #checkValid()} passes
     */
    public X509Certificate peerCertificate()
    {
        return peerCertificate;
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

    private X509Certificate getPeerCertificate()
    {
        // First certificate in the chain is peer's own cert
        if (!certificateChain.isEmpty() && certificateChain.get(0) instanceof X509Certificate)
        {
            return (X509Certificate) certificateChain.get(0);
        }

        return null;
    }
}
