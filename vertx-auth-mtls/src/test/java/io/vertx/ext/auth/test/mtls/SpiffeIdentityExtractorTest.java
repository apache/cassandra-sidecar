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

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.impl.SpiffeIdentityExtractor;

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
                .addSanUriName("spiffe://test.cassandra.apache.org/unitTest/mtls")
                .buildSelfSigned();
        assertThat(identityExtractor.identity(new Certificate[]{certificate})).isEqualTo("spiffe://test.cassandra.apache.org/unitTest/mtls");
    }

    @Test
    public void testDifferentCertificateType()
    {
        Certificate mockCertificate = mock(Certificate.class);
        assertThatThrownBy(() -> identityExtractor.identity(new Certificate[]{mockCertificate})).isInstanceOf(CredentialValidationException.class);
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
        assertThatThrownBy(() -> identityExtractor.identity(new Certificate[]{certificate}))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessage("Unable to extract valid Spiffe ID from certificate");
    }

    @Test
    public void testInvalidCertificate() throws Exception
    {
        X509Certificate certificate
                = CertificateBuilder
                .builder()
                .issuerName("CN=Vertx Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                .buildSelfSigned();
        assertThatThrownBy(() -> identityExtractor.identity(new Certificate[]{certificate}))
                .isInstanceOf(CredentialValidationException.class)
                .hasMessage("Unable to extract valid Spiffe ID from certificate");
    }
}
