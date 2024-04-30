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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.common.request.Request;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link NoRetryPolicy} class
 */
class NoRetryPolicyTest
{
    Request mockRequest;
    HttpResponse mockResponse;
    RetryPolicy retryPolicy;

    @BeforeEach
    public void setup()
    {
        mockRequest = mock(Request.class);
        mockResponse = mock(HttpResponse.class);
        retryPolicy = new NoRetryPolicy();
        when(mockRequest.requestURI()).thenReturn("/api/uri");
    }

    @Test
    void testStatusCode200() throws ExecutionException, InterruptedException
    {
        when(mockResponse.statusCode()).thenReturn(OK.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        retryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, false,
                               (attempts, retryDelayMillis) -> fail("Should never retry"));
        future.join();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isSameAs(mockResponse);
    }

    @Test
    void testFailsWhenThrowableIsProvided()
    {
        when(mockResponse.statusCode()).thenReturn(OK.code()); // status code is irrelevant

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        retryPolicy.onResponse(future, mockRequest, mockResponse, new IllegalArgumentException("this fails"), 1, false,
                               (attempts, retryDelayMillis) -> fail("Should never retry"));

        assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(future::join)
        .withCauseInstanceOf(RetriesExhaustedException.class)
        .withMessageContaining("Unable to complete request '/api/uri' after 1 attempt");
        assertThat(future.isCompletedExceptionally()).isTrue();
    }

    @Test
    void testFailsWithNon200StatusCode()
    {
        when(mockResponse.statusCode()).thenReturn(INTERNAL_SERVER_ERROR.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        retryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, false,
                               (attempts, retryDelayMillis) -> fail("Should never retry"));

        assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(future::join)
        .withCauseInstanceOf(RetriesExhaustedException.class)
        .withMessageContaining("Unable to complete request '/api/uri' after 1 attempt");
        assertThat(future.isCompletedExceptionally()).isTrue();
    }
}
