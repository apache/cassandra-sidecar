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

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.CertificateIdentityExtractor;

/**
 * {@link CertificateIdentityExtractor} implementation for SPIFFE certificates for extracting valid SPIFFE identity.
 * SPIFFE is a URI, present as part of SAN of client certificates, it uniquely identifies client.
 */
public class SpiffeIdentityExtractor implements CertificateIdentityExtractor
{
    // SpiffeIdentityExtractor can extract and validate only URI type SAN identities. As per RFC 5280 standards,
    // here https://datatracker.ietf.org/doc/html/rfc5280#section-4.2.1.6 URI type is represented with number 6.
    private static final int SUBJECT_ALT_NAME_URI_TYPE = 6;
    private static final String SPIFFE_PREFIX = "spiffe://";
    private final String trustedDomain;

    public SpiffeIdentityExtractor()
    {
        this(null);
    }

    public SpiffeIdentityExtractor(String trustedDomain)
    {
        this.trustedDomain = trustedDomain;
    }

    @Override
    public String validIdentity(CertificateCredentials certificateCredentials) throws CredentialValidationException
    {
        // First certificate in certificate chain is usually PrivateKeyEntry.
        X509Certificate privateCert = certificateCredentials.peerCertificate();

        if (privateCert == null)
        {
            throw new CredentialValidationException("No X509Certificate found for validating");
        }

        String identity = extractIdentity(privateCert);
        validateIdentity(identity);
        return identity;
    }

    protected String extractIdentity(X509Certificate certificate)
    {
        try
        {
            Collection<List<?>> subjectAltNames = certificate.getSubjectAlternativeNames();
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
        catch (Exception e)
        {
            throw new CredentialValidationException("Error reading SAN of certificate", e);
        }
        throw new CredentialValidationException("Unable to extract SPIFFE identity from certificate");
    }

    protected void validateIdentity(String identity)
    {
        verifyPrefix(identity);
        if (trustedDomain != null)
        {
            verifyDomain(identity);
        }
    }

    private void verifyPrefix(String identity)
    {
        if (!identity.startsWith(SPIFFE_PREFIX))
        {
            throw new CredentialValidationException("Spiffe identity must start with prefix " + SPIFFE_PREFIX);
        }
    }

    private void verifyDomain(String identity)
    {
        String uriSuffix = identity.replaceFirst(SPIFFE_PREFIX, "");
        String[] uriSuffixParts = uriSuffix.split("/");
        boolean domainPresentCheck = uriSuffixParts.length > 0;

        if (!domainPresentCheck)
        {
            throw new CredentialValidationException("Spiffe identity extracted " + identity + " does not contain domain information");
        }

        String domain = uriSuffixParts[0];
        if (!domain.equals(trustedDomain))
        {
            throw new CredentialValidationException("Spiffe Identity domain " + domain + " is not trusted");
        }
    }
}
