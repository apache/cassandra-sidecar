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
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.mtls.CertificateIdentityExtractor;
import io.vertx.ext.auth.mtls.CertificateValidator;
import io.vertx.ext.auth.mtls.MutualTlsAuthentication;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MutualTlsAuthenticationImpl}
 */
@ExtendWith(VertxExtension.class)
class MutualTlsAuthenticationProviderTest
{
    private static final CertificateValidator ALLOW_ALL_CERTIFICATE_VALIDATOR = new AllowAllCertificateValidator();
    Vertx vertx;
    MutualTlsAuthentication mTlsAuth;
    SelfSignedCertificate validCert;

    @BeforeEach
    void setUp() throws CertificateException
    {
        vertx = Vertx.vertx();
        validCert = new SelfSignedCertificate();
    }

    @AfterEach
    void cleanup()
    {
        vertx.close();
    }

    @Test
    public void testSuccess(VertxTestContext context)
    {
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);

        mTlsAuth = MutualTlsAuthentication.create(vertx, ALLOW_ALL_CERTIFICATE_VALIDATOR, mockIdentityExtracter);
        List<Certificate> certChain = Collections.singletonList(validCert.cert());
        CertificateCredentials credentials = new CertificateCredentials(certChain);

        when(mockIdentityExtracter.validIdentities(credentials)).thenReturn(Collections.singletonList("default"));

        mTlsAuth.authenticate(credentials)
                .onFailure(res -> context.failNow("mTls should have succeeded"))
                .onSuccess(res -> context.completeNow());
    }

    @Test
    public void testWithTokenCredentials(VertxTestContext context)
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, mockCertificateValidator, mockIdentityExtracter);

        TokenCredentials creds = new TokenCredentials();

        mTlsAuth.authenticate(creds)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("CertificateCredentials expected for mTLS authentication");
                    context.completeNow();
                }));
    }

    @Test
    public void testValidCertificate(VertxTestContext context) throws Exception
    {
        CertificateValidator certificateValidator
        = new CertificateValidatorImpl(Collections.singleton("Vertx Auth"), "oss", "ssl_test", "US");
        CertificateIdentityExtractor identityExtracter = new SpiffeIdentityExtractor();

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, certificateValidator, identityExtracter);

        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=oss, L=Unknown, ST=Unknown, C=US")
                                      .addSanUriName("spiffe://vertx.auth/unitTest/mtls")
                                      .buildSelfSigned()
                                      .certificate();
        List<Certificate> certChain = Collections.singletonList(certificate);
        CertificateCredentials credentials = new CertificateCredentials(certChain);

        mTlsAuth.authenticate(credentials)
                .onFailure(res -> context.failNow("mTls should have succeeded"))
                .onSuccess(res -> context.completeNow());
    }

    @Test
    public void testInvalidCertificate(VertxTestContext context) throws Exception
    {
        CertificateValidator certificateValidator
        = new CertificateValidatorImpl(Collections.singleton("Vertx Auth"), "oss", "ssl_test", "US");
        CertificateIdentityExtractor identityExtracter = new SpiffeIdentityExtractor();

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, certificateValidator, identityExtracter);

        Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=oss, L=Unknown, ST=Unknown, C=US")
                                      .addSanUriName("spiffe://vertx.auth/unitTest/mtls")
                                      .notAfter(yesterday)
                                      .buildSelfSigned()
                                      .certificate();
        List<Certificate> certChain = Collections.singletonList(certificate);
        CertificateCredentials credentials = new CertificateCredentials(certChain);

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Expired certificates shared for authentication");
                    context.completeNow();
                });
    }

    @Test
    public void testUnknownExceptionInCertertificateValidation(VertxTestContext context)
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, mockCertificateValidator, mockIdentityExtracter);
        Certificate mockCertificate = mock(Certificate.class);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(mockCertificate));

        doThrow(new RuntimeException("Invalid certificate")).when(mockCertificateValidator).verifyCertificate(credentials);

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Invalid certificate");
                    context.completeNow();
                });
    }

    @Test
    public void testUnknownExceptionInIdentityExtraction(VertxTestContext context)
    {
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, ALLOW_ALL_CERTIFICATE_VALIDATOR, mockIdentityExtracter);
        List<Certificate> certChain = Collections.singletonList(validCert.cert());
        CertificateCredentials credentials = new CertificateCredentials(certChain);

        when(mockIdentityExtracter.validIdentities(credentials)).thenThrow(new RuntimeException("Bad Identity"));

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Bad Identity");
                    context.completeNow();
                });
    }

    @Test
    public void testEmptyIdentity(VertxTestContext context) throws Exception
    {
        CertificateValidator certificateValidator
        = new CertificateValidatorImpl(Collections.singleton("Vertx Auth"), "oss", "ssl_test", "US");
        CertificateIdentityExtractor identityExtracter = new SpiffeIdentityExtractor();

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, certificateValidator, identityExtracter);

        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=oss, L=Unknown, ST=Unknown, C=US")
                                      .addSanUriName("")
                                      .buildSelfSigned()
                                      .certificate();
        List<Certificate> certChain = Collections.singletonList(certificate);
        CertificateCredentials credentials = new CertificateCredentials(certChain);

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Error reading SAN of certificate");
                    context.completeNow();
                });
    }

    @Test
    public void testInvalidIdentity(VertxTestContext context) throws Exception
    {
        CertificateValidator certificateValidator
        = new CertificateValidatorImpl(Collections.singleton("Vertx Auth"), "oss", "ssl_test", "US");
        CertificateIdentityExtractor identityExtracter = new SpiffeIdentityExtractor();

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, certificateValidator, identityExtracter);

        X509Certificate certificate = new CertificateBuilder()
                                      .subject("CN=Vertx Auth, OU=ssl_test, O=oss, L=Unknown, ST=Unknown, C=US")
                                      .addSanUriName("badIdentity")
                                      .buildSelfSigned()
                                      .certificate();
        List<Certificate> certChain = Collections.singletonList(certificate);
        CertificateCredentials credentials = new CertificateCredentials(certChain);

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Error reading SAN of certificate");
                    context.completeNow();
                }));
    }

    @Test
    public void testAuthenticateWithJson()
    {
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);

        mTlsAuth = new MutualTlsAuthenticationImpl(vertx, ALLOW_ALL_CERTIFICATE_VALIDATOR, mockIdentityExtracter);
        JsonObject json = new JsonObject();

        assertThatThrownBy(() -> mTlsAuth.authenticate(json, user -> {
        }))
        .isInstanceOf(UnsupportedOperationException.class);
    }
}
