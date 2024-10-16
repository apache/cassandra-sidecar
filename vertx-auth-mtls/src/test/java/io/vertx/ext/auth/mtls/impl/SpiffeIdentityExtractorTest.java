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

import io.vertx.ext.auth.mtls.utils.CertificateBuilder;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link io.vertx.ext.auth.mtls.impl.SpiffeIdentityExtractor}
 */
public class SpiffeIdentityExtractorTest
{
    SpiffeIdentityExtractor identityExtractor = new SpiffeIdentityExtractor();

    @Test
    public void testSpiffeIdentity() throws Exception
    {
        X509Certificate certificate
        = CertificateBuilder
          .builder()
          .issuerName("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
          .addSanUriName("spiffe://vertx.auth/unitTest/mtls")
          .buildSelfSigned();
        assertThat(identityExtractor.validIdentity(new CertificateCredentials(certificate)))
        .isEqualTo("spiffe://vertx.auth/unitTest/mtls");
    }

    @Test
    public void testDifferentCertificateType()
    {
        Certificate mockCertificate = mock(Certificate.class);
        assertThatThrownBy(() -> identityExtractor.validIdentity(new CertificateCredentials(mockCertificate)))
        .isInstanceOf(CredentialValidationException.class);
    }

    @Test
    public void testNonSpiffeIdentity() throws Exception
    {
        X509Certificate certificate
        = CertificateBuilder
          .builder()
          .issuerName("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
          .addSanUriName("randomuri://extracted/from/certificate")
          .buildSelfSigned();
        assertThatThrownBy(() -> identityExtractor.validIdentity(new CertificateCredentials(certificate)))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("Unable to extract SPIFFE identity from certificate");
    }

    @Test
    public void testInvalidCertificate() throws Exception
    {
        X509Certificate certificate
        = CertificateBuilder
          .builder()
          .issuerName("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
          .buildSelfSigned();
        assertThatThrownBy(() -> identityExtractor.validIdentity(new CertificateCredentials(certificate)))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("Error reading SAN of certificate");
    }

    @Test
    public void testNonTrustedDomain() throws Exception
    {
        X509Certificate certificate
        = CertificateBuilder
          .builder()
          .issuerName("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
          .addSanUriName("spiffe://nontrusted/unitTest/mtls")
          .buildSelfSigned();
        SpiffeIdentityExtractor identityExtractorWithTrust = new SpiffeIdentityExtractor("vertx.auth");
        assertThatThrownBy(() -> identityExtractorWithTrust.validIdentity(new CertificateCredentials(certificate)))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("SPIFFE Identity domain nontrusted is not trusted");
    }

    @Test
    public void testNonX509CertificatePeerCertificate()
    {
        Certificate certificate = mock(Certificate.class);
        assertThatThrownBy(() -> identityExtractor.validIdentity(new CertificateCredentials(certificate)))
        .isInstanceOf(CredentialValidationException.class)
        .hasMessage("No X509Certificate found for validating");
    }
}
