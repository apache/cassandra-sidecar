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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.client.request.Request;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExponentialBackoffRetryPolicyTest
{
    Request mockRequest;
    HttpResponse mockResponse;

    @BeforeEach
    public void setup()
    {
        mockRequest = mock(Request.class);
        mockResponse = mock(HttpResponse.class);
        when(mockRequest.requestURI()).thenReturn("/api/uri");
    }

    @Test
    void testOverflows()
    {
        long delay = new ExponentialBackoffRetryPolicy(1, 1, 0)
                     .retryDelayMillis(64); // 2 ^ (64 - 1) + 1 overflows
        assertThat(delay).as("Should not overflow when the exponential grows larger")
                         .isEqualTo(Long.MAX_VALUE);

        delay = new ExponentialBackoffRetryPolicy(1, 2, 0)
                .retryDelayMillis(63); // 2 * 2 ^ (63 - 1) + 1 overflows
        assertThat(delay).isEqualTo(Long.MAX_VALUE);

        delay = new ExponentialBackoffRetryPolicy(1, 200, 0)
                .retryDelayMillis(56);
        assertThat(delay).as("If the number of retries is greater than or equals to 56, the value will overflow")
                         .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testCompletesWithOKStatusCode() throws ExecutionException, InterruptedException
    {
        when(mockResponse.statusCode()).thenReturn(OK.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        new ExponentialBackoffRetryPolicy().onResponse(future, mockRequest, mockResponse, null, 1, false,
                                                       (attempts, retryDelayMillis) -> fail("Should never retry"));
        future.join();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isSameAs(mockResponse);
    }

    @ParameterizedTest(name = "{index} => canRetryOnADifferentHost={0}")
    @ValueSource(booleans = { true, false })
    void testRetriesWhenThrowableIsProvided(boolean canRetryOnADifferentHost)
    {
        IllegalArgumentException throwable = new IllegalArgumentException("Connection Refused");
        testWithRetries(mockRequest, null, throwable, 10, 100,
                        canRetryOnADifferentHost ? 0 : 100, 1_000, canRetryOnADifferentHost);
    }

    @Test
    void testExponentialBackoffOnServerError()
    {
        when(mockResponse.statusCode()).thenReturn(INTERNAL_SERVER_ERROR.code());
        testWithRetries(mockRequest, mockResponse, null, 5, 200, 200, 10_000, false);
        // should not exceed the configuredMaxRetryDelayMillis
        testWithRetries(mockRequest, mockResponse, null, 30, 200, 200, 10_000, false);
        testWithRetries(mockRequest, mockResponse, null, 10, 150, 0, 1_000, true);
        // no maximum configured for max retry delay millis
        testWithRetries(mockRequest, mockResponse, null, 30, 200, 200, -1, false);
    }

    private static void testWithRetries(Request request, HttpResponse response, Throwable throwable,
                                        int configuredMaxRetries, int configuredRetryDelayMillis,
                                        int originalExpectedRetryDelayMillis,
                                        int configuredMaxRetryDelayMillis, boolean canRetryOnADifferentHost)
    {
        RetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy(configuredMaxRetries, configuredRetryDelayMillis,
                                                                    configuredMaxRetryDelayMillis);
        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        AtomicLong expectedRetryDelayMillis = new AtomicLong(originalExpectedRetryDelayMillis);
        for (int currentAttempt = 1; currentAttempt <= configuredMaxRetries; currentAttempt++)
        {
            // retries on error
            int expectedNextAttempt = currentAttempt + 1;
            retryPolicy.onResponse(future, request, response, throwable, currentAttempt, canRetryOnADifferentHost,
                                   (attempts, retryDelayMillis) -> {
                                       if (expectedNextAttempt > configuredMaxRetries)
                                       {
                                           fail("Should never retry");
                                       }
                                       else
                                       {
                                           assertThat(attempts).isEqualTo(expectedNextAttempt);
                                           assertThat(retryDelayMillis).isEqualTo(expectedRetryDelayMillis.get());
                                       }
                                   });
            expectedRetryDelayMillis.updateAndGet(current -> configuredMaxRetryDelayMillis > 0
                                                             ? Math.min(configuredMaxRetryDelayMillis, current * 2)
                                                             : current * 2);
        }

        assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(future::join)
        .withCauseInstanceOf(RetriesExhaustedException.class)
        .withMessageContaining("Unable to complete request '/api/uri' after " + configuredMaxRetries + " attempts");
        assertThat(future.isCompletedExceptionally()).isTrue();
    }
}
