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
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.mtls.utils.CertificateBuilder;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.CertificateValidator;

import static io.vertx.ext.auth.authentication.CertificateCredentialsTest.createTestCredentials;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl}
 */
public class CertificateValidatorImplTest
{
    private final CertificateValidator certificateValidator = CertificateValidatorImpl.builder()
                                                                                      .trustedCNs(Collections.singleton("Vertx Auth"))
                                                                                      .trustedIssuerOrganization("Vertx")
                                                                                      .trustedIssuerOrganizationUnit("ssl_test")
                                                                                      .trustedIssuerCountry("US")
                                                                                      .build();

    @Test
    public void testValidCertificateCredentials()
    {
        CertificateCredentials credentials = createTestCredentials();
        certificateValidator.verifyCertificate(credentials);
    }

    @Test
    public void testInvalidCertificateType()
    {
        Certificate certificate = mock(Certificate.class);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        assertThatThrownBy(() -> certificateValidator.verifyCertificate(credentials))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("No X509Certificate found for validating");
    }

    @Test
    public void testNonTrustedIssuer()
    {
        CertificateCredentials credentials = createTestCredentials("CN=Vertx Auth, OU=ssl_test, " +
                                                                   "O=NonTrustedOrganization, " +
                                                                   "L=Unknown, ST=Unknown, C=US");
        assertThatThrownBy(() -> certificateValidator.verifyCertificate(credentials))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("NonTrustedOrganization attribute not trusted");
    }

    @Test
    public void testInvalidIssuer()
    {
        CertificateValidator certificateValidator
        = CertificateValidatorImpl.builder()
                                  .trustedCNs(Collections.singleton("Vertx Auth"))
                                  .trustedIssuerOrganization("MissingIssuerOrganization").trustedIssuerOrganizationUnit("ssl_test")
                                  .trustedIssuerCountry("US").build();
        CertificateCredentials credentials = createTestCredentials("CN=Vertx Auth, OU=ssl_test, L=Unknown, ST=Unknown, C=US");
        assertThatThrownBy(() -> certificateValidator.verifyCertificate(credentials))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("Expected attribute O not found");
    }

    @Test
    public void testExpiredCertificate() throws Exception
    {
        X509Certificate certificate
        = CertificateBuilder.builder()
                            .notAfter(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)))
                            .issuerName("CN=Vertx Auth, OU=ssl_test, O=Vertx, L=Unknown, ST=Unknown, C=US").buildSelfSigned();
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        assertThatThrownBy(() -> certificateValidator.verifyCertificate(credentials))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("Expired certificates shared for authentication");
    }
}
