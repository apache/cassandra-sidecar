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
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.mtls.CertificateIdentityExtractor;
import io.vertx.ext.auth.mtls.CertificateIdentityValidator;
import io.vertx.ext.auth.mtls.CertificateValidator;
import io.vertx.ext.auth.mtls.impl.MutualTlsAuthenticationProvider;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MutualTlsAuthenticationProvider}
 */
@ExtendWith(VertxExtension.class)
public class MutualTlsAuthenticationTest
{
    MutualTlsAuthenticationProvider mTlsAuth;
    SelfSignedCertificate validCert;

    @BeforeEach
    public void setUp() throws CertificateException, InterruptedException
    {
        validCert = new SelfSignedCertificate();
        VertxTestContext context = new VertxTestContext();
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSuccess(VertxTestContext context) throws Exception
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);
        CertificateIdentityValidator mockIdentityValidator = mock(CertificateIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityExtracter, mockIdentityValidator);
        Certificate[] certChain = {validCert.cert()};
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(validCert.cert()));

        when(mockCertificateValidator.isValidCertificate(credentials)).thenReturn(true);
        when(mockIdentityExtracter.identity(certChain)).thenReturn("spiffe://testcert/auth/mtls");
        when(mockIdentityValidator.isValidIdentity("spiffe://testcert/auth/mtls")).thenReturn(true);

        mTlsAuth.authenticate(credentials)
                .onFailure(res -> context.failNow("mTls should have succeeded"))
                .onSuccess(res -> context.completeNow());
    }

    @Test
    public void testWithTokenCredentials(VertxTestContext context)
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);
        CertificateIdentityValidator mockIdentityValidator = mock(CertificateIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityExtracter, mockIdentityValidator);

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
    public void testInvalidCertificate(VertxTestContext context) throws Exception
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);
        CertificateIdentityValidator mockIdentityValidator = mock(CertificateIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityExtracter, mockIdentityValidator);
        Certificate mockCertificate = mock(Certificate.class);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(mockCertificate));

        when(mockCertificateValidator.isValidCertificate(credentials)).thenThrow(new RuntimeException("Invalid certificate"));

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Invalid certificate passed");
                    context.completeNow();
                });
    }

    @Test
    public void testNullIdentity(VertxTestContext context) throws Exception
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);
        CertificateIdentityValidator mockIdentityValidator = mock(CertificateIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityExtracter, mockIdentityValidator);
        Certificate[] certChain = Collections.singleton((Certificate) validCert.cert()).toArray(new Certificate[0]);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(validCert.cert()));

        when(mockCertificateValidator.isValidCertificate(credentials)).thenReturn(true);
        when(mockIdentityExtracter.identity(certChain)).thenReturn(null);

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Could not extract identity from certificate");
                    context.completeNow();
                }));
    }

    @Test
    public void testEmptyIdentity(VertxTestContext context) throws Exception
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);
        CertificateIdentityValidator mockIdentityValidator = mock(CertificateIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityExtracter, mockIdentityValidator);
        Certificate[] certChain = Collections.singleton((Certificate) validCert.cert()).toArray(new Certificate[0]);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(validCert.cert()));

        when(mockCertificateValidator.isValidCertificate(credentials)).thenReturn(true);
        when(mockIdentityExtracter.identity(certChain)).thenReturn("");

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Could not extract identity from certificate");
                    context.completeNow();
                }));
    }

    @Test
    public void testInvalidIdentity(VertxTestContext context) throws Exception
    {
        CertificateValidator mockCertificateValidator = mock(CertificateValidator.class);
        CertificateIdentityExtractor mockIdentityExtracter = mock(CertificateIdentityExtractor.class);
        CertificateIdentityValidator mockIdentityValidator = mock(CertificateIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityExtracter, mockIdentityValidator);
        Certificate[] certChain = Collections.singleton((Certificate) validCert.cert()).toArray(new Certificate[0]);
        CertificateCredentials credentials = new CertificateCredentials(Collections.singletonList(validCert.cert()));

        when(mockCertificateValidator.isValidCertificate(credentials)).thenReturn(true);
        when(mockIdentityExtracter.identity(certChain)).thenReturn("badIdentity");
        when(mockIdentityValidator.isValidIdentity("badIdentity")).thenReturn(false);

        mTlsAuth.authenticate(credentials)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Certificate identity is not a valid identity");
                    context.completeNow();
                }));
    }
}
