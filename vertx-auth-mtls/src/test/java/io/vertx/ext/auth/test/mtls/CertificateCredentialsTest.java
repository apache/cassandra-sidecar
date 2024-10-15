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
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link CertificateCredentials}
 */
public class CertificateCredentialsTest
{
    @Test
    public void testEmptyCertificateChain()
    {
        List<Certificate> certificateChain = Collections.emptyList();
        assertThatThrownBy(() -> new CertificateCredentials(certificateChain).checkValid())
                .isInstanceOf(CredentialValidationException.class);
    }

    @Test
    public void testNonCertificateBasedConnection()
    {
        HttpServerRequest request = mock(HttpServerRequest.class);
        HttpConnection connection = mock(HttpConnection.class);
        when(request.connection()).thenReturn(connection);

        assertThatThrownBy(() -> new CertificateCredentials(request).checkValid())
                .isInstanceOf(CredentialValidationException.class);
    }
}
