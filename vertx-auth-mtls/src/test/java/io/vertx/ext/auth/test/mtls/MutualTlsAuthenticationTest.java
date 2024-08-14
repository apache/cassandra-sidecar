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
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.mtls.MutualTlsAuthenticationProvider;
import io.vertx.ext.auth.mtls.MutualTlsCertificateValidator;
import io.vertx.ext.auth.mtls.MutualTlsCredentials;
import io.vertx.ext.auth.mtls.MutualTlsIdentityValidator;
import io.vertx.ext.auth.mtls.exceptions.AuthenticationException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test class for mTls authentication. Tests {@link MutualTlsAuthenticationProvider}
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
    public void testMTlsSuccess(VertxTestContext context) throws AuthenticationException
    {
        MutualTlsCertificateValidator mockCertificateValidator = mock(MutualTlsCertificateValidator.class);
        MutualTlsIdentityValidator mockIdentityValidator = mock(MutualTlsIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityValidator);
        Certificate[] certChain = Collections.singleton((Certificate) validCert.cert()).toArray(new Certificate[0]);

        MutualTlsCredentials creds = new MutualTlsCredentials(Arrays.asList(certChain));

        when(mockCertificateValidator.isValidCertificate(creds)).thenReturn(true);
        when(mockCertificateValidator.identity(certChain)).thenReturn("validIdentity");
        when(mockIdentityValidator.isValidIdentity("validIdentity")).thenReturn(true);
        when(mockIdentityValidator.userFromIdentity("validIdentity")).thenReturn(null);

        mTlsAuth.authenticate(creds)
                .onFailure(res -> context.failNow("mTls should have succeeded"))
                .onSuccess(res -> context.completeNow());
    }

    @Test
    public void testNonMutualTlsCredentialsPassed(VertxTestContext context)
    {
        MutualTlsCertificateValidator mockCertificateValidator = mock(MutualTlsCertificateValidator.class);
        MutualTlsIdentityValidator mockIdentityValidator = mock(MutualTlsIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityValidator);

        TokenCredentials creds = new TokenCredentials();

        mTlsAuth.authenticate(creds)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Unable to authenticate");
                    context.completeNow();
                }));
    }

    @Test
    public void testInvalidCertificate(VertxTestContext context) throws AuthenticationException
    {
        MutualTlsCertificateValidator mockCertificateValidator = mock(MutualTlsCertificateValidator.class);
        MutualTlsIdentityValidator mockIdentityValidator = mock(MutualTlsIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityValidator);
        Certificate[] certChain = Collections.singleton((Certificate) validCert.cert()).toArray(new Certificate[0]);

        MutualTlsCredentials creds = new MutualTlsCredentials(Arrays.asList(certChain));

        when(mockCertificateValidator.isValidCertificate(creds)).thenReturn(false);
        when(mockCertificateValidator.identity(certChain)).thenReturn("badIdentity");
        when(mockIdentityValidator.userFromIdentity("badIdentity")).thenReturn(null);
        when(mockIdentityValidator.isValidIdentity("badIdentity")).thenReturn(true);

        mTlsAuth.authenticate(creds)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> {
                    assert (res != null);
                    assert (res.getMessage().contains("Unable to authenticate"));
                    context.completeNow();
                });
    }

    @Test
    public void testInvalidIdentity(VertxTestContext context) throws AuthenticationException
    {
        MutualTlsCertificateValidator mockCertificateValidator = mock(MutualTlsCertificateValidator.class);
        MutualTlsIdentityValidator mockIdentityValidator = mock(MutualTlsIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityValidator);
        Certificate[] certChain = Collections.singleton((Certificate) validCert.cert()).toArray(new Certificate[0]);

        MutualTlsCredentials creds = new MutualTlsCredentials(Arrays.asList(certChain));

        when(mockCertificateValidator.isValidCertificate(creds)).thenReturn(true);
        when(mockCertificateValidator.identity(certChain)).thenReturn("validIdentity");
        when(mockIdentityValidator.userFromIdentity("validIdentity")).thenReturn(null);
        when(mockIdentityValidator.isValidIdentity("validIdentity")).thenReturn(false);

        mTlsAuth.authenticate(creds)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Unable to authenticate");
                    context.completeNow();
                }));
    }

    @Test
    public void testNoIdentity(VertxTestContext context) throws AuthenticationException
    {
        MutualTlsCertificateValidator mockCertificateValidator = mock(MutualTlsCertificateValidator.class);
        MutualTlsIdentityValidator mockIdentityValidator = mock(MutualTlsIdentityValidator.class);

        mTlsAuth = new MutualTlsAuthenticationProvider(mockCertificateValidator, mockIdentityValidator);
        Certificate[] certChain = Collections.singleton((Certificate) validCert.cert()).toArray(new Certificate[0]);

        MutualTlsCredentials creds = new MutualTlsCredentials(Arrays.asList(certChain));

        when(mockCertificateValidator.isValidCertificate(creds)).thenReturn(true);
        when(mockCertificateValidator.identity(certChain)).thenReturn("");
        when(mockIdentityValidator.userFromIdentity("validIdentity")).thenReturn(null);
        when(mockIdentityValidator.isValidIdentity("validIdentity")).thenReturn(true);

        mTlsAuth.authenticate(creds)
                .onSuccess(res -> context.failNow("Should have failed"))
                .onFailure(res -> context.verify(() -> {
                    assertThat(res).isNotNull();
                    assertThat(res.getMessage()).contains("Unable to authenticate");
                    context.completeNow();
                }));
    }
}
