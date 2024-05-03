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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.common.request.Request;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A retry policy that ignores status code {@link HttpResponseStatus#CONFLICT} ({@code 409}), and assumes success
 * when that status code is returned by the server.
 */
public class IgnoreConflictRetryPolicy extends ExponentialBackoffRetryPolicy
{
    @VisibleForTesting
    IgnoreConflictRetryPolicy()
    {
        super();
    }

    /**
     * Constructs a new instance of this class
     *
     * @param maxRetries          the maximum number of retries
     * @param retryDelayMillis    the delay between retries in milliseconds
     * @param maxRetryDelayMillis the maximum retry delay in milliseconds
     */
    public IgnoreConflictRetryPolicy(int maxRetries, long retryDelayMillis, long maxRetryDelayMillis)
    {
        super(maxRetries, retryDelayMillis, maxRetryDelayMillis);
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
        if (response != null && response.statusCode() == HttpResponseStatus.CONFLICT.code())
        {
            responseFuture.complete(response);
        }
        else
        {
            super.onResponse(responseFuture, request, response, throwable, attempts, canRetryOnADifferentHost,
                             retryAction);
        }
    }
}
