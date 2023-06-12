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

package org.apache.cassandra.sidecar.client.request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.NodeSettings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for the {@link DecodableRequest} class
 */
class DecodableRequestTest
{
    DecodableRequest<NodeSettings> instance;

    @BeforeEach
    void setup()
    {
        instance = new DecodableRequest<NodeSettings>("https://cassandra-sidecar.com/api/test")
        {
            @Override
            public HttpMethod method()
            {
                return HttpMethod.GET;
            }
        };
    }

    @Test
    void testDecode() throws IOException
    {
        String nodeSettingsAsJsonString = "{\"partitioner\":\"partitioner-value\",\"releaseVersion\":\"1.0-TEST\"}";

        NodeSettings nodeSettings = instance.decode(nodeSettingsAsJsonString.getBytes(StandardCharsets.UTF_8));
        assertThat(nodeSettings.partitioner()).isEqualTo("partitioner-value");
        assertThat(nodeSettings.releaseVersion()).isEqualTo("1.0-TEST");
    }


    @Test
    void testDecodeIgnoresUnknownProperties() throws IOException
    {
        String nodeSettingsAsJsonString = "{\"partitioner\":\"partitioner-value\",\"releaseVersion\":\"1.0-TEST\"," +
                                          "\"newProperty\":\"some-value\"}";

        NodeSettings nodeSettings = instance.decode(nodeSettingsAsJsonString.getBytes(StandardCharsets.UTF_8));
        assertThat(nodeSettings.partitioner()).isEqualTo("partitioner-value");
        assertThat(nodeSettings.releaseVersion()).isEqualTo("1.0-TEST");
    }

    @Test
    void testHeadersAreImmutable()
    {
        assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> instance.headers().put("not", "allowed"));
    }
}
