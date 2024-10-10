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

package io.vertx.ext.auth.test.mtls;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.CertificateValidator;
import io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl}
 */
public class CertificateValidatorImplTest
{
    @Test
    public void testValidCertificateCredentials() throws Exception
    {
        CertificateValidator certificateValidator
                = CertificateValidatorImpl.builder().trustedCNs(Collections.singleton("Vertx Auth"))
                .trustedIssuerOrganization("Vertx").trustedIssuerOrganizationUnit("ssl_test")
                .trustedIssuerCountry("US").build();
        X509Certificate certificate
                = CertificateBuilder.builder()
                .issuerName("CN=Vertx Auth, OU=ssl_test, O=Vertx, L=Unknown, ST=Unknown, C=US").buildSelfSigned();
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        assertThat(certificateValidator.isValidCertificate(credentials)).isTrue();
    }

    @Test
    public void testInvalidCertificateType() throws Exception
    {
        CertificateValidator certificateValidator
                = CertificateValidatorImpl.builder().trustedCNs(Collections.singleton("Vertx Auth"))
                .trustedIssuerOrganization("Vertx").trustedIssuerOrganizationUnit("ssl_test")
                .trustedIssuerCountry("US").build();
        Certificate certificate = mock(Certificate.class);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        assertThat(certificateValidator.isValidCertificate(credentials)).isFalse();
    }

    @Test
    public void testNonTrustedIssuer() throws Exception
    {
        CertificateValidator certificateValidator
                = CertificateValidatorImpl.builder().trustedCNs(Collections.singleton("Vertx Auth"))
                .trustedIssuerOrganization("Vertx").trustedIssuerOrganizationUnit("ssl_test")
                .trustedIssuerCountry("US").build();
        X509Certificate certificate
                = CertificateBuilder.builder()
                .issuerName("CN=Vertx Auth, OU=ssl_test, O=NonTrustedOrganization, L=Unknown, ST=Unknown, C=US").buildSelfSigned();
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        assertThat(certificateValidator.isValidCertificate(credentials)).isFalse();
    }

    @Test
    public void testInvalidIssuer() throws Exception
    {
        CertificateValidator certificateValidator
                = CertificateValidatorImpl.builder().trustedCNs(Collections.singleton("Vertx Auth"))
                .trustedIssuerOrganization("MissingIssuerOrganization").trustedIssuerOrganizationUnit("ssl_test")
                .trustedIssuerCountry("US").build();
        X509Certificate certificate
                = CertificateBuilder.builder()
                .issuerName("CN=Vertx Auth, OU=ssl_test, L=Unknown, ST=Unknown, C=US").buildSelfSigned();
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        assertThatThrownBy(() -> certificateValidator.isValidCertificate(credentials)).isInstanceOf(CredentialValidationException.class);
    }

    @Test
    public void testExpiredCertificate() throws Exception
    {
        CertificateValidator certificateValidator
                = CertificateValidatorImpl.builder().trustedCNs(Collections.singleton("Vertx Auth"))
                .trustedIssuerOrganization("Vertx").trustedIssuerOrganizationUnit("ssl_test")
                .trustedIssuerCountry("US").build();
        X509Certificate certificate
                = CertificateBuilder.builder()
                .notAfter(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)))
                .issuerName("CN=Vertx Auth, OU=ssl_test, O=Vertx, L=Unknown, ST=Unknown, C=US").buildSelfSigned();
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        assertThatThrownBy(() -> certificateValidator.isValidCertificate(credentials))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessage("Expired certificates shared for authentication");
    }
}
