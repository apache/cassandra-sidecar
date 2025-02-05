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

package org.apache.cassandra.sidecar.routes;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.vertx.core.http.HttpServerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractHandler#extractHostAddressWithoutPort} functionality
 */
class ExtractHostAddressWithoutPortTest
{
    @ParameterizedTest(name = "{index} => input={0}, expected={1}")
    @MethodSource(value = { "inputs" })
    void testHostExtraction(String input, String expectedValue)
    {
        String host = AbstractHandler.extractHostAddressWithoutPort(mockRequest(input));
        assertThat(host).isEqualTo(expectedValue);
    }

    @Test
    void testExtractNullInput()
    {
        assertThatThrownBy(() -> AbstractHandler.extractHostAddressWithoutPort(mockRequest(null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing 'host' header in the request");
    }

    static Stream<Arguments> inputs()
    {
        return Stream.of(
        arguments("127.0.0.1", "127.0.0.1"),
        arguments("127.0.0.1:9043", "127.0.0.1"),
        arguments("2001:db8:0:0:0:ff00:42:8329", "2001:db8:0:0:0:ff00:42:8329"),
        arguments("[2001:db8:0:0:0:ff00:42:8329]:9043", "2001:db8:0:0:0:ff00:42:8329"),
        arguments("::1", "::1"),
        arguments("[::1]:9043", "::1"),
        arguments("www.apache.cassandra.sidecar.com", "www.apache.cassandra.sidecar.com"),
        arguments("www.apache.cassandra.sidecar.com:9043", "www.apache.cassandra.sidecar.com")
        );
    }

    private HttpServerRequest mockRequest(String host)
    {
        HttpServerRequest req = mock(HttpServerRequest.class);
        when(req.host()).thenReturn(host);
        return req;
    }
}
