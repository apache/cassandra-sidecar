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

package org.apache.cassandra.sidecar.common.utils;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.cassandra.sidecar.common.utils.HttpEncodings.decodeSSTableComponent;
import static org.apache.cassandra.sidecar.common.utils.HttpEncodings.encodeSSTableComponent;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Unit tests for {@link HttpEncodings}
 */
class HttpEncodingsTest
{
    @Test
    void testNullEncodeSSTableComponent()
    {
        assertThatThrownBy(() -> encodeSSTableComponent(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("componentName must be provided");
    }

    @ParameterizedTest(name = "{index} => decoded={0}, expected={1}")
    @MethodSource("componentValuesArguments")
    void testEncodeSSTableComponent(String decoded, String encoded)
    {
        assertThat(encodeSSTableComponent(decoded)).isEqualTo(encoded);
    }

    @Test
    void testNullDecodeSStableComponent()
    {
        assertThatThrownBy(() -> decodeSSTableComponent(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("encodedComponentName must be provided");
    }

    @ParameterizedTest(name = "{index} => encoded={1}, expected={0}")
    @MethodSource("componentValuesArguments")
    void testDecodeSStableComponent(String decoded, String encoded)
    {
        assertThat(decodeSSTableComponent(encoded)).isEqualTo(decoded);
    }

    private static Stream<Arguments> componentValuesArguments()
    {
        return Stream.of(
        Arguments.of("", ""),
        Arguments.of("nc-1-big-Data.db", "nc-1-big-Data.db"),
        Arguments.of("nc-1-big-TOC.txt", "nc-1-big-TOC.txt"),
        Arguments.of(".ryear/nc-1-big-Data.db", ".ryear%2Fnc-1-big-Data.db"),
        Arguments.of(".ryear/nc-1-big-TOC.txt", ".ryear%2Fnc-1-big-TOC.txt")
        );
    }
}
