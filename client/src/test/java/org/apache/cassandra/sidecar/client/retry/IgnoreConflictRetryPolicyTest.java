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

package org.apache.cassandra.sidecar.client.retry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.client.request.Request;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IgnoreConflictRetryPolicy}
 */
class IgnoreConflictRetryPolicyTest
{
    Request mockRequest;
    HttpResponse mockResponse;
    Map<String, List<String>> headersMap;

    @BeforeEach
    void setup()
    {
        headersMap = new HashMap<>();
        mockRequest = mock(Request.class);
        mockResponse = mock(HttpResponse.class);
        when(mockResponse.headers()).thenReturn(headersMap);
    }

    @ParameterizedTest(name = "{index} => canRetryOnADifferentHost={0}")
    @ValueSource(booleans = { true, false })
    void testRetryPolicyWhenResponseIsNull(boolean canRetryOnADifferentHost)
    {
        int maxRetries = 2;

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        IgnoreConflictRetryPolicy retryPolicy = new IgnoreConflictRetryPolicy(maxRetries, 100, 100);

        for (int currentAttempt = 1; currentAttempt <= maxRetries; currentAttempt++)
        {
            int expectedNextAttempt = currentAttempt + 1;
            retryPolicy.onResponse(future, mockRequest, null, new RuntimeException(), currentAttempt,
                                   canRetryOnADifferentHost, (attempts, retryDelayMillis) -> {
                if (expectedNextAttempt > maxRetries)
                {
                    fail("Should not retry");
                }
                else
                {
                    assertThat(attempts).isEqualTo(expectedNextAttempt);
                    if (canRetryOnADifferentHost)
                    {
                        assertThat(retryDelayMillis).isEqualTo(0);
                    }
                    else
                    {
                        assertThat(retryDelayMillis).isEqualTo(100);
                    }
                }
            });
        }
        assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(future::join)
        .withCauseInstanceOf(RetriesExhaustedException.class);
        assertThat(future.isCompletedExceptionally()).isTrue();
    }

    @Test
    void testCompletesWithConflictStatusCode() throws ExecutionException, InterruptedException
    {
        when(mockResponse.statusCode()).thenReturn(CONFLICT.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        new IgnoreConflictRetryPolicy().onResponse(future, mockRequest, mockResponse, null, 1, false,
                                                   (attempts, retryDelayMillis) -> fail("Should never retry"));
        future.join();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isSameAs(mockResponse);
    }
}
