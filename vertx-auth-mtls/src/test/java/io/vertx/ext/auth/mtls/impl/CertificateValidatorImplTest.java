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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.CertificateValidator;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;

import static io.vertx.ext.auth.authentication.CertificateCredentialsTest.createTestCredentials;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link io.vertx.ext.auth.mtls.impl.CertificateValidatorImpl}
 */
@ExtendWith(VertxExtension.class)
public class CertificateValidatorImplTest
{
    private final CertificateValidator certificateValidator = CertificateValidatorImpl.builder()
                                                                                      .trustedCNs(Collections.singleton("Vertx Auth"))
                                                                                      .trustedIssuerOrganization("Vertx")
                                                                                      .trustedIssuerOrganizationUnit("ssl_test")
                                                                                      .trustedIssuerCountry("US")
                                                                                      .build();

    @Test
    public void testValidCertificateCredentials(VertxTestContext context)
    {
        CertificateCredentials credentials = createTestCredentials();
        certificateValidator.verifyCertificate(credentials)
                           .onFailure(res -> context.failNow("Certificate validation should have succeeded"))
                           .onSuccess(res -> context.completeNow());
    }

    @Test
    public void testInvalidCertificateType(VertxTestContext context)
    {
        Certificate certificate = mock(Certificate.class);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        certificateValidator.verifyCertificate(credentials)
                            .onSuccess(res -> context.failNow("Should have failed"))
                            .onFailure(res -> context.verify(() -> {
                                assertThat(res).isInstanceOf(CredentialValidationException.class);
                                assertThat(res.getMessage())
                                .isEqualTo("No X509Certificate found for validating");
                                context.completeNow();
                            }));
    }

    @Test
    public void testNonTrustedIssuer(VertxTestContext context)
    {
        CertificateCredentials credentials = createTestCredentials("CN=Vertx Auth, OU=ssl_test, " +
                                                                   "O=NonTrustedOrganization, " +
                                                                   "L=Unknown, ST=Unknown, C=US");
        certificateValidator.verifyCertificate(credentials)
                            .onSuccess(res -> context.failNow("Should have failed"))
                            .onFailure(res -> context.verify(() -> {
                                assertThat(res).isInstanceOf(CredentialValidationException.class);
                                assertThat(res.getMessage())
                                .isEqualTo("NonTrustedOrganization attribute not trusted");
                                context.completeNow();
                           }));
    }

    @Test
    public void testInvalidIssuer(VertxTestContext context)
    {
        CertificateValidator certificateValidator
        = CertificateValidatorImpl.builder()
                                  .trustedCNs(Collections.singleton("Vertx Auth"))
                                  .trustedIssuerOrganization("MissingIssuerOrganization").trustedIssuerOrganizationUnit("ssl_test")
                                  .trustedIssuerCountry("US").build();
        CertificateCredentials credentials = createTestCredentials("CN=Vertx Auth, OU=ssl_test, L=Unknown, ST=Unknown, C=US");
        certificateValidator.verifyCertificate(credentials)
                            .onSuccess(res -> context.failNow("Should have failed"))
                            .onFailure(res -> context.verify(() -> {
                                assertThat(res).isInstanceOf(CredentialValidationException.class);
                                assertThat(res.getMessage()).isEqualTo("Expected attribute O not found");
                                context.completeNow();
                            }));
    }

    @Test
    public void testExpiredCertificate(VertxTestContext context) throws Exception
    {
        X509Certificate certificate
        = new CertificateBuilder().notAfter(Instant.now().minus(1, ChronoUnit.DAYS))
                                  .subject("CN=Vertx Auth, OU=ssl_test, O=Vertx, L=Unknown, ST=Unknown, C=US")
                                  .buildSelfSigned()
                                  .certificate();
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(certificate));
        certificateValidator.verifyCertificate(credentials)
                            .onSuccess(res -> context.failNow("Should have failed"))
                            .onFailure(res -> context.verify(() -> {
                                assertThat(res).isInstanceOf(CredentialValidationException.class);
                                assertThat(res.getMessage()).isEqualTo("Expired certificates shared for authentication");
                                context.completeNow();
                            }));
    }
}
