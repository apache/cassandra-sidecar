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

package org.apache.cassandra.sidecar.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.Http1xServerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * RequestUtilsTest
 */
class RequestUtilsTest
{
    HttpServerRequest mockRequest;

    @BeforeEach
    void setup()
    {
        mockRequest = mock(Http1xServerRequest.class);
    }

    @Test
    void testParseBooleanQueryParam()
    {
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "non-existent", true)).isTrue();
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "non-existent-false", false)).isFalse();

        when(mockRequest.getParam("false-param")).thenReturn("false");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "false-param", true)).isFalse();

        when(mockRequest.getParam("fAlSe-mixed-case-param")).thenReturn("fAlSe");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "fAlSe-mixed-case-param", true)).isFalse();

        when(mockRequest.getParam("FALSE-uppercase-param")).thenReturn("FALSE");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "FALSE-uppercase-param", true)).isFalse();

        when(mockRequest.getParam("true-param")).thenReturn("true");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "true-param", false)).isTrue();

        when(mockRequest.getParam("TrUe-mixed-case-param")).thenReturn("TrUe");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "TrUe-mixed-case-param", false)).isTrue();

        when(mockRequest.getParam("TRUE-uppercase-param")).thenReturn("TRUE");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "TRUE-uppercase-param", false)).isTrue();

        when(mockRequest.getParam("default-value-true")).thenReturn("not-a-valid-true");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "default-value-true", false)).isFalse();

        when(mockRequest.getParam("default-value-false")).thenReturn("not-a-valid-false");
        assertThat(RequestUtils.parseBooleanQueryParam(mockRequest, "default-value-false", true)).isTrue();
    }
}
