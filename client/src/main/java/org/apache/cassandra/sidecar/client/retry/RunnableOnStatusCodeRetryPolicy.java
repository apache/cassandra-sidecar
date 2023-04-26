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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.request.Request;

/**
 * A delegate retry policy that executes the Runnable every configured number of requests retries on the
 * configured status code.
 */
public class RunnableOnStatusCodeRetryPolicy extends RetryPolicy
{
    private final RetryPolicy delegate;
    private final AtomicInteger recordedAttempts = new AtomicInteger(0);
    private final int statusCode;
    private final int numberOfEntriesToSkip;
    private final Runnable runnable;

    public RunnableOnStatusCodeRetryPolicy(Runnable runnable, RetryPolicy delegate, int statusCode)
    {
        this(runnable, delegate, statusCode, 10);
    }

    public RunnableOnStatusCodeRetryPolicy(Runnable runnable,
                                           RetryPolicy delegate,
                                           int statusCode,
                                           int numberOfEntriesToSkip)
    {
        this.runnable = runnable;
        this.delegate = delegate;
        this.statusCode = statusCode;
        this.numberOfEntriesToSkip = numberOfEntriesToSkip;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onResponse(CompletableFuture<HttpResponse> responseFuture,
                           Request request,
                           HttpResponse response,
                           Throwable throwable,
                           int attempts,
                           boolean canRetryOnADifferentHost,
                           RetryAction retryAction)
    {
        if (response.statusCode() == statusCode
            && recordedAttempts.getAndIncrement() % numberOfEntriesToSkip == 0)
        {
            runnable.run();
        }

        delegate.onResponse(responseFuture, request, response, throwable, attempts, canRetryOnADifferentHost,
                            retryAction);
    }
}
