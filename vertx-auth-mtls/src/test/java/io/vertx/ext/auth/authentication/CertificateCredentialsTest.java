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

package io.vertx.ext.auth.authentication;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.SSLPeerUnverifiedException;

import org.junit.jupiter.api.Test;

import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link CertificateCredentials}
 */
public class CertificateCredentialsTest
{
    @Test
    void testValidCertificate()
    {
        assertThatNoException().isThrownBy(() -> createTestCredentials().checkValid());
    }

    @Test
    void testEmptyCertificateChain()
    {
        List<Certificate> certificateChain = Collections.emptyList();
        assertThatThrownBy(() -> new CertificateCredentials(certificateChain).checkValid())
        .isInstanceOf(CredentialValidationException.class);
    }

    @Test
    void testNonCertificateBasedConnection() throws SSLPeerUnverifiedException
    {
        HttpServerRequest mockRequest = mock(HttpServerRequest.class);
        HttpConnection mockConnection = mock(HttpConnection.class);
        when(mockRequest.connection()).thenReturn(mockConnection);
        when(mockConnection.peerCertificates()).thenThrow(new RuntimeException("Unexpected error"));
        assertThat(CertificateCredentials.fromHttpRequest(mockRequest)).isNull();
    }

    @Test
    void testToJson()
    {
        Certificate certificate = mock(Certificate.class);
        CertificateCredentials credentials = new CertificateCredentials(certificate);
        assertThatThrownBy(credentials::toJson)
        .isInstanceOf(UnsupportedOperationException.class);
    }

    public static CertificateCredentials createTestCredentials()
    {
        return createTestCredentials("CN=Vertx Auth, OU=ssl_test, O=Vertx, L=Unknown, ST=Unknown, C=US");
    }

    public static CertificateCredentials createTestCredentials(String issuerName)
    {
        try
        {
            X509Certificate certificate = new CertificateBuilder().subject(issuerName)
                                                                  .buildSelfSigned()
                                                                  .certificate();
            return new CertificateCredentials(Collections.singletonList(certificate));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
