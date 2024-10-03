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

import io.vertx.ext.auth.mtls.CertificateIdentityValidator;

/**
 * {@link CertificateIdentityValidator} implementation for SPIFFE certificates for validating SPIFFE identities.
 * SPIFFE is a URI, present as part of SAN of client certificates, it uniquely identifies client.
 */
public class SpiffeIdentityValidator implements CertificateIdentityValidator
{
    private static final String SPIFFE_ID_PREFIX = "spiffe://";
    private final String trustedDomain;

    public SpiffeIdentityValidator(String trustedDomain)
    {
        this.trustedDomain = trustedDomain;
    }

    @Override
    public boolean isValidIdentity(String identity)
    {
        return isValidSpiffeId(identity) && isDomainTrusted(identity);
    }

    private boolean isValidSpiffeId(String identity)
    {
        return identity.startsWith(SPIFFE_ID_PREFIX);
    }

    private boolean isDomainTrusted(String identity)
    {
        String uriSuffix = identity.replaceFirst(SPIFFE_ID_PREFIX, "");
        String[] uriSuffixParts = uriSuffix.split("/");
        boolean domainPresentCheck = uriSuffixParts.length > 0;

        if (!domainPresentCheck)
        {
            return false;
        }

        String domain = uriSuffixParts[0];
        return domain.equals(trustedDomain);
    }
}
