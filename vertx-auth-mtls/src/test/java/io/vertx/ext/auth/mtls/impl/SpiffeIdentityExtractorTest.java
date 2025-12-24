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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link io.vertx.ext.auth.mtls.impl.SpiffeIdentityExtractor}
 */
@ExtendWith(VertxExtension.class)
public class SpiffeIdentityExtractorTest
{
    SpiffeIdentityExtractor identityExtractor = new SpiffeIdentityExtractor();

    @Test
    public void testSpiffeIdentity(VertxTestContext context) throws Exception
    {
        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                      .addSanUriName("spiffe://vertx.auth/unitTest/mtls")
                                      .buildSelfSigned()
                                      .certificate();
        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onFailure(res -> context.failNow("Identity extraction should have succeeded"))
                         .onSuccess(identities -> context.verify(() -> {
                             assertThat(identities).contains("spiffe://vertx.auth/unitTest/mtls");
                             context.completeNow();
                         }));
    }

    @Test
    public void testDifferentCertificateType(VertxTestContext context)
    {
        Certificate mockCertificate = mock(Certificate.class);
        identityExtractor.validIdentities(new CertificateCredentials(mockCertificate))
                         .onSuccess(res -> context.failNow("Should have failed"))
                         .onFailure(res -> context.verify(() -> {
                             assertThat(res).isInstanceOf(CredentialValidationException.class);
                             context.completeNow();
                         }));
    }

    @Test
    public void testNonSpiffeIdentity(VertxTestContext context) throws Exception
    {
        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                      .addSanUriName("randomuri://extracted/from/certificate")
                                      .buildSelfSigned()
                                      .certificate();
        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onSuccess(res -> context.failNow("Should have failed"))
                         .onFailure(res -> context.verify(() -> {
                             assertThat(res).isInstanceOf(CredentialValidationException.class);
                             assertThat(res.getMessage())
                             .isEqualTo("Unable to extract SPIFFE identity from certificate");
                             context.completeNow();
                        }));
    }

    @Test
    public void testInvalidCertificate(VertxTestContext context) throws Exception
    {
        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                      .buildSelfSigned()
                                      .certificate();
        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onSuccess(res -> context.failNow("Should have failed"))
                         .onFailure(res -> context.verify(() -> {
                             assertThat(res).isInstanceOf(CredentialValidationException.class);
                             assertThat(res.getMessage()).isEqualTo("Error reading SAN of certificate");
                             context.completeNow();
                         }));
    }

    @Test
    public void testNonTrustedDomain(VertxTestContext context) throws Exception
    {
        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                      .addSanUriName("spiffe://nontrusted/unitTest/mtls")
                                      .buildSelfSigned()
                                      .certificate();
        SpiffeIdentityExtractor identityExtractorWithTrust = new SpiffeIdentityExtractor("vertx.auth");
        identityExtractorWithTrust.validIdentities(new CertificateCredentials(certificate))
                                  .onSuccess(res -> context.failNow("Should have failed"))
                                  .onFailure(res -> context.verify(() -> {
                                      assertThat(res).isInstanceOf(CredentialValidationException.class);
                                      assertThat(res.getMessage())
                                      .isEqualTo("SPIFFE Identity domain nontrusted is not trusted");
                                      context.completeNow();
                                 }));
    }

    @Test
    public void testNonX509CertificatePeerCertificate(VertxTestContext context)
    {
        Certificate certificate = mock(Certificate.class);
        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onSuccess(res -> context.failNow("Should have failed"))
                         .onFailure(res -> context.verify(() -> {
                             assertThat(res).isInstanceOf(CredentialValidationException.class);
                             assertThat(res.getMessage()).isEqualTo("No X509Certificate found for validating");
                             context.completeNow();
                        }));
    }
}
