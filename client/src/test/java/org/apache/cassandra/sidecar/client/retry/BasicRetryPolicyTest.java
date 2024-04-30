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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.exception.ResourceNotFoundException;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.client.exception.UnexpectedStatusCodeException;
import org.apache.cassandra.sidecar.common.request.Request;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.sidecar.common.http.SidecarHttpResponseStatus.CHECKSUM_MISMATCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link BasicRetryPolicy}
 */
class BasicRetryPolicyTest
{
    HttpResponse mockResponse;
    Request mockRequest;
    RetryPolicy defaultBasicRetryPolicy;
    Map<String, List<String>> headersMap;

    @BeforeEach
    void setup()
    {
        defaultBasicRetryPolicy = new BasicRetryPolicy();
        headersMap = new HashMap<>();
        mockResponse = mock(HttpResponse.class);
        mockRequest = mock(Request.class);
        when(mockResponse.headers()).thenReturn(headersMap);
        when(mockRequest.requestURI()).thenReturn("/api/uri");
    }

    @ParameterizedTest(name = "{index} => canRetryOnADifferentHost={0}")
    @ValueSource(booleans = { true, false })
    void testRetriesWhenThrowableIsProvided(boolean canRetryOnADifferentHost)
    {
        IllegalArgumentException throwable = new IllegalArgumentException("Connection Refused");
        testWithRetries(mockRequest, null, throwable, 3, 100, canRetryOnADifferentHost);
    }

    @Test
    void testRetriesOnInternalServerErrorStatusCode()
    {
        when(mockResponse.statusCode()).thenReturn(INTERNAL_SERVER_ERROR.code());
        testWithRetries(mockRequest, mockResponse, null, 5, 200, false);
        testWithRetries(mockRequest, mockResponse, null, 5, 200, true);
    }

    @Test
    void testCompletesWithOKStatusCode() throws ExecutionException, InterruptedException
    {
        when(mockResponse.statusCode()).thenReturn(OK.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        defaultBasicRetryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, false,
                                           (attempts, retryDelayMillis) -> fail("Should never retry"));
        future.join();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isSameAs(mockResponse);
    }

    @ParameterizedTest(name = "{index} => canRetryOnADifferentHost={0}")
    @ValueSource(booleans = { true, false })
    void testCompletesWithNotFoundStatusCode(boolean canRetryOnADifferentHost)
    throws ExecutionException, InterruptedException
    {
        when(mockResponse.statusCode()).thenReturn(NOT_FOUND.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        if (canRetryOnADifferentHost)
        {
            testWithRetries(mockRequest, mockResponse, null, 3, 50, true);
        }
        else
        {
            defaultBasicRetryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, canRetryOnADifferentHost,
                                               (attempts, retryDelayMillis) -> fail("Should never retry"));
            assertThatExceptionOfType(CompletionException.class)
            .isThrownBy(future::join)
            .withCauseInstanceOf(ResourceNotFoundException.class);
        }
    }

    @ParameterizedTest(name = "{index} => canRetryOnADifferentHost={0}")
    @ValueSource(booleans = { true, false })
    void testRetriesWithNotImplementedStatusCode(boolean canRetryOnADifferentHost)
    {
        when(mockResponse.statusCode()).thenReturn(NOT_IMPLEMENTED.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        defaultBasicRetryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, canRetryOnADifferentHost,
                                           (attempts, retryDelayMillis) -> fail("Should never retry"));

        assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(future::join)
        .withCauseInstanceOf(UnsupportedOperationException.class);
        assertThat(future.isCompletedExceptionally()).isTrue();
    }

    @Test
    void testRetriesWithNotAcceptableStatusCode()
    {
        when(mockResponse.statusCode()).thenReturn(NOT_ACCEPTABLE.code());
        testWithRetries(mockRequest, mockResponse, null, 8, 50, true);
    }

    @ParameterizedTest(name = "{index} => canRetryOnADifferentHost={0}")
    @ValueSource(booleans = { true, false })
    void testRetriesWithServiceUnavailableStatusCode(boolean canRetryOnADifferentHost)
    {
        when(mockResponse.statusCode()).thenReturn(SERVICE_UNAVAILABLE.code());
        testWithRetries(mockRequest, mockResponse, null, 5, 300, canRetryOnADifferentHost);
    }

    @ParameterizedTest(name = "{index} => canRetryOnADifferentHost={0}")
    @ValueSource(booleans = { true, false })
    void testRetriesWithChecksumMismatchStatusCode(boolean canRetryOnADifferentHost)
    {
        when(mockResponse.statusCode()).thenReturn(CHECKSUM_MISMATCH.code());
        testWithRetries(mockRequest, mockResponse, null, 5, 300, canRetryOnADifferentHost);
    }

    @Test
    void testRetriesWithServiceUnavailableStatusCodeWithRetryAfterHeader()
    {
        when(mockResponse.statusCode()).thenReturn(SERVICE_UNAVAILABLE.code());
        headersMap.put("Retry-After", Collections.singletonList("5")); // 5 seconds -> 5,000 millis
        testWithRetries(mockRequest, mockResponse, null, 5, 1000, 5000, false);
        testWithRetries(mockRequest, mockResponse, null, 5, 1000, 0, true);
    }

    @Test
    void testRetriesWithServiceUnavailableStatusCodeWithInvalidRetryAfterHeader()
    {
        when(mockResponse.statusCode()).thenReturn(SERVICE_UNAVAILABLE.code());
        headersMap.put("Retry-After", Collections.singletonList("one-thousand-seconds"));
        testWithRetries(mockRequest, mockResponse, null, 5, 200, 200, false);
        testWithRetries(mockRequest, mockResponse, null, 5, 200, 0, true);
    }

    @Test
    void testRetriesWithAcceptedStatusCode() throws ExecutionException, InterruptedException
    {
        when(mockResponse.statusCode()).thenReturn(ACCEPTED.code());
        RetryPolicy retryPolicy = new BasicRetryPolicy(1);
        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        AtomicInteger retryActionCalls = new AtomicInteger(0);
        int maxRetries = 50;
        for (int currentAttempt = 1; currentAttempt <= maxRetries; currentAttempt++)
        {
            retryPolicy.onResponse(future, mockRequest, mockResponse, null, currentAttempt, true,
                                   (attempts, retryDelayMillis) -> {
                                       retryActionCalls.incrementAndGet();
                                       assertThat(attempts).isEqualTo(1);
                                       assertThat(retryDelayMillis).isEqualTo(0);
                                   });
        }

        when(mockResponse.statusCode()).thenReturn(OK.code());

        retryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, false,
                               (attempts, retryDelayMillis) -> fail("Should not retry"));
        future.join();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isSameAs(mockResponse);
        assertThat(retryActionCalls.get()).isEqualTo(maxRetries);
    }

    private static Stream<Arguments> clientStatusCodeArguments()
    {
        return IntStream.range(400, 500)
                        .filter(statusCode -> statusCode != NOT_FOUND.code()
                                              && statusCode != CHECKSUM_MISMATCH.code())
                        .boxed()
                        .map(Arguments::of);
    }

    @ParameterizedTest(name = "{index} => statusCode={0}")
    @MethodSource("clientStatusCodeArguments")
    void testRetriesWithClientError(int statusCode)
    {
        when(mockResponse.statusCode()).thenReturn(statusCode);
        testWithRetries(mockRequest, mockResponse, null, 2, 200, 200, false, false);
        testWithRetries(mockRequest, mockResponse, null, 2, 200, 0, true, false);
    }

    private static Stream<Arguments> serverStatusCodeArguments()
    {
        return IntStream.range(500, 600)
                        .filter(statusCode -> statusCode != INTERNAL_SERVER_ERROR.code()
                                              && statusCode != NOT_IMPLEMENTED.code()
                                              && statusCode != SERVICE_UNAVAILABLE.code())
                        .boxed()
                        .map(Arguments::of);
    }

    @ParameterizedTest(name = "{index} => statusCode={0}")
    @MethodSource("serverStatusCodeArguments")
    void testRetriesWithServerError(int statusCode)
    {
        when(mockResponse.statusCode()).thenReturn(statusCode);
        testWithRetries(mockRequest, mockResponse, null, 3, 200, 200, false, false);
        testWithRetries(mockRequest, mockResponse, null, 3, 200, 0, true, false);
    }

    @Test
    void testFailsWithUnexpectedStatusCode()
    {
        when(mockResponse.statusCode()).thenReturn(700); // 700 is an invalid status code

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        defaultBasicRetryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, false,
                                           (attempts, retryDelayMillis) -> fail("Should never retry"));
        assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(future::join)
        .withCauseInstanceOf(UnexpectedStatusCodeException.class)
        .withMessageContaining("Unexpected HTTP status code 700");
        assertThat(future.isCompletedExceptionally()).isTrue();
    }

    private static void testWithRetries(Request request, HttpResponse response, Throwable throwable,
                                        int configuredMaxRetries, int configuredRetryDelayMillis,
                                        boolean canRetryOnADifferentHost)
    {
        testWithRetries(request, response, throwable, configuredMaxRetries, configuredRetryDelayMillis,
                        canRetryOnADifferentHost ? 0 : configuredRetryDelayMillis, canRetryOnADifferentHost);
    }

    private static void testWithRetries(Request request, HttpResponse response, Throwable throwable,
                                        int configuredMaxRetries, int configuredRetryDelayMillis,
                                        int expectedRetryDelayMillis, boolean canRetryOnADifferentHost)
    {
        testWithRetries(request, response, throwable, configuredMaxRetries, configuredRetryDelayMillis,
                        expectedRetryDelayMillis, canRetryOnADifferentHost, true);
    }

    private static void testWithRetries(Request request, HttpResponse response, Throwable throwable,
                                        int configuredMaxRetries, int configuredRetryDelayMillis,
                                        int expectedRetryDelayMillis, boolean canRetryOnADifferentHost,
                                        boolean expectedToRetryOnSameHost)
    {
        RetryPolicy retryPolicy = new BasicRetryPolicy(configuredMaxRetries, configuredRetryDelayMillis);
        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
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
                                           assertThat(retryDelayMillis).isEqualTo(expectedRetryDelayMillis);
                                       }
                                   });

            if (!canRetryOnADifferentHost && !expectedToRetryOnSameHost)
            {
                break;
            }
        }

        String description;
        if (expectedToRetryOnSameHost || canRetryOnADifferentHost)
        {
            description = "Unable to complete request '/api/uri' after " + configuredMaxRetries + " attempts";
        }
        else
        {
            description = "Unable to complete request '/api/uri' after 1 attempt";
        }

        assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(future::join)
        .withCauseInstanceOf(RetriesExhaustedException.class)
        .withMessageContaining(description);
        assertThat(future.isCompletedExceptionally()).isTrue();
    }
}
