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

import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.CertificateIdentityExtractor;

import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link CertificateIdentityExtractor} implementation for SPIFFE certificates for extracting SPIFFE identity.
 * SPIFFE is a URI, present as part of SAN of client certificates, it uniquely identifies client.
 */
public class SpiffeIdentityExtractor implements CertificateIdentityExtractor
{
    private static final int SUBJECT_ALT_NAME_URI_TYPE = 6;
    private static final String SPIFFE_PREFIX = "spiffe://";

    @Override
    public String identity(Certificate[] certificateChain) throws CredentialValidationException, CertificateParsingException
    {
        X509Certificate[] castedCerts = castCertsToX509(certificateChain);
        if (castedCerts.length == 0)
        {
            throw new CredentialValidationException("No based X509Certificate found for validating");
        }
        X509Certificate privateCert = castedCerts[0];

        Collection<List<?>> subjectAltNames = privateCert.getSubjectAlternativeNames();
        if (subjectAltNames != null && !subjectAltNames.isEmpty())
        {
            for (List<?> item : subjectAltNames)
            {
                Integer type = (Integer) item.get(0);
                String identity = (String) item.get(1);
                if (type == SUBJECT_ALT_NAME_URI_TYPE && identity.startsWith(SPIFFE_PREFIX))
                {
                    return identity;
                }
            }
        }

        throw new CredentialValidationException("Unable to extract valid Spiffe ID from certificate");
    }

    /**
     * Filters instances of {@link X509Certificate} certificates and returns the certificate chain as
     * {@link X509Certificate} certificates.
     *
     * @param certificateChain client certificate chain
     * @return an array of {@link X509Certificate} certificates
     */
    private X509Certificate[] castCertsToX509(Certificate[] certificateChain) throws CredentialValidationException
    {
        if (certificateChain == null || certificateChain.length == 0)
        {
            throw new CredentialValidationException("Certificate chain shared is empty");
        }
        return Arrays.stream(certificateChain).filter(certificate -> certificate instanceof X509Certificate).toArray(X509Certificate[]::new);
    }
}
