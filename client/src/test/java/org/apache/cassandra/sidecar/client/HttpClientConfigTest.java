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

package org.apache.cassandra.sidecar.client;

import java.io.InputStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link HttpClientConfig}
 */
class HttpClientConfigTest
{
    @Test
    void testBuilderDefaults()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().build();
        assertThat(config.timeoutMillis()).isEqualTo(30_000);
        assertThat(config.ssl()).isEqualTo(true);
        assertThat(config.maxPoolSize()).isEqualTo(20);
        assertThat(config.userAgent()).isEqualTo("sidecar-client/1.0.0");
        assertThat(config.idleTimeoutMillis()).isEqualTo(0);
        assertThat(config.maxChunkSize()).isEqualTo(6291456);
        assertThat(config.receiveBufferSize()).isEqualTo(-1);
        assertThat(config.sendReadBufferSize()).isEqualTo(8192);
        assertThat(config.trustStoreInputStream()).isNull();
        assertThat(config.trustStorePassword()).isNull();
        assertThat(config.trustStoreType()).isEqualTo("JKS");
        assertThat(config.keyStoreInputStream()).isNull();
        assertThat(config.keyStorePassword()).isNull();
        assertThat(config.keyStoreType()).isEqualTo("PKCS12");
    }

    @Test
    void testBuilderTimeout()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().timeoutMillis(5_000).build();
        assertThat(config.timeoutMillis()).isEqualTo(5_000);
    }

    @Test
    void testBuilderSsl()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().ssl(true).build();
        assertThat(config.ssl()).isTrue();
        config = new HttpClientConfig.Builder<>().ssl(false).build();
        assertThat(config.ssl()).isFalse();
    }

    @Test
    void testMaxPoolSize()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().maxPoolSize(120).build();
        assertThat(config.maxPoolSize()).isEqualTo(120);
    }

    @Test
    void testUserAgent()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().userAgent("test-user-agent/5.0.0").build();
        assertThat(config.userAgent()).isEqualTo("test-user-agent/5.0.0");
    }

    @Test
    void testIdleTimeoutMillis()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().idleTimeoutMillis(5_555).build();
        assertThat(config.idleTimeoutMillis()).isEqualTo(5_555);
    }

    @Test
    void testMaxChunkSize()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().maxChunkSize(7_250).build();
        assertThat(config.maxChunkSize()).isEqualTo(7_250);
    }

    @Test
    void testReceiveBufferSize()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().receiveBufferSize(250).build();
        assertThat(config.receiveBufferSize()).isEqualTo(250);
    }

    @Test
    void testReadBufferSize()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().readBufferSize(188).build();
        assertThat(config.sendReadBufferSize()).isEqualTo(188);
    }

    @Test
    void testTrustStoreInputStream()
    {
        InputStream mockInputStream = mock(InputStream.class);
        HttpClientConfig config = new HttpClientConfig.Builder<>().trustStoreInputStream(mockInputStream).build();
        assertThat(config.trustStoreInputStream()).isEqualTo(mockInputStream);
    }

    @Test
    void testTrustStorePassword()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().trustStorePassword("do-not-use-password").build();
        assertThat(config.trustStorePassword()).isEqualTo("do-not-use-password");
    }

    @Test
    void testTrustStoreType()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().trustStoreType("CUSTOM_TYPE").build();
        assertThat(config.trustStoreType()).isEqualTo("CUSTOM_TYPE");
    }

    @Test
    void testKeystoreInputStream()
    {
        InputStream mockInputStream = mock(InputStream.class);
        HttpClientConfig config = new HttpClientConfig.Builder<>().keyStoreInputStream(mockInputStream).build();
        assertThat(config.keyStoreInputStream()).isEqualTo(mockInputStream);
    }

    @Test
    void testKeystorePassword()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().keyStorePassword("do-not-use-password").build();
        assertThat(config.keyStorePassword()).isEqualTo("do-not-use-password");
    }

    @Test
    void testKeystoreType()
    {
        HttpClientConfig config = new HttpClientConfig.Builder<>().keyStoreType("CUSTOM_TYPE").build();
        assertThat(config.keyStoreType()).isEqualTo("CUSTOM_TYPE");
    }
}
