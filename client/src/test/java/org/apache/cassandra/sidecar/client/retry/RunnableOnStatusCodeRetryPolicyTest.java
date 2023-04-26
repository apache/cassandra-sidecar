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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.request.Request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link RunnableOnStatusCodeRetryPolicy} class
 */
class RunnableOnStatusCodeRetryPolicyTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RunnableOnStatusCodeRetryPolicyTest.class);

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

    @Test
    void testExecutesEveryTwoTimes()
    {
        when(mockResponse.statusCode()).thenReturn(HttpResponseStatus.OK.code());
        AtomicInteger count = new AtomicInteger();
        Runnable incrementEveryTwoTimes = count::incrementAndGet;

        RetryPolicy retryPolicy = new RunnableOnStatusCodeRetryPolicy(incrementEveryTwoTimes,
                                                                      new BasicRetryPolicy(),
                                                                      HttpResponseStatus.OK.code(),
                                                                      2);

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        for (int i = 0; i < 19; i++)
        {
            retryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, false,
                                   (attempts, retryDelayMillis) -> fail("Should never retry"));
        }
        assertThat(count.get()).isEqualTo(10);
    }

    @Test
    void testExecutesEveryTenTimes()
    {
        when(mockResponse.statusCode()).thenReturn(HttpResponseStatus.OK.code());
        AtomicInteger count = new AtomicInteger();
        Runnable incrementEveryTenTimes = count::incrementAndGet;

        RetryPolicy retryPolicy = new RunnableOnStatusCodeRetryPolicy(incrementEveryTenTimes,
                                                                      new BasicRetryPolicy(),
                                                                      HttpResponseStatus.OK.code());

        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        for (int i = 0; i < 20; i++)
        {
            retryPolicy.onResponse(future, mockRequest, mockResponse, null, 1, false,
                                   (attempts, retryDelayMillis) -> fail("Should never retry"));
        }
        assertThat(count.get()).isEqualTo(2);
    }
}
