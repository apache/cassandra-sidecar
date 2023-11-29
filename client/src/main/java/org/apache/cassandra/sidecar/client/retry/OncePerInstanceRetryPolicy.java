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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.client.request.Request;
import org.apache.cassandra.sidecar.common.utils.TimeUtils;

/**
 * A retry policy that attempts to execute the request once on each instance
 * until the first successful response is received, or fails if none were successful.
 *
 * Accepts optional minimum and maximum durations used to calculate random delay
 * before each retry attempt in order to avoid the thundering herd problem.
 *
 * Retries immediately without delay if minimum and maximum durations are not specified.
 */
public class OncePerInstanceRetryPolicy extends RetryPolicy
{
    private final Duration minimumDelay;
    private final Duration maximumDelay;

    /**
     * Instantiates {@link OncePerInstanceRetryPolicy} with no delay between retry attempts
     */
    public OncePerInstanceRetryPolicy()
    {
        this(Duration.ZERO, Duration.ZERO);
    }

    /**
     * Instantiates {@link OncePerInstanceRetryPolicy} with random delays between retry attempts
     *
     * @param minimumDelay duration of minimum possible retry delay, inclusive
     * @param maximumDelay duration of maximum possible retry delay, inclusive
     */
    public OncePerInstanceRetryPolicy(Duration minimumDelay, Duration maximumDelay)
    {
        super();
        this.minimumDelay = minimumDelay;
        this.maximumDelay = maximumDelay;
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
        if (response != null && response.statusCode() == HttpResponseStatus.OK.code())
        {
            responseFuture.complete(response);
        }
        else if (canRetryOnADifferentHost)
        {
            retryAction.retry(attempts + 1, TimeUtils.randomDuration(minimumDelay, maximumDelay).toMillis());
        }
        else
        {
            responseFuture.completeExceptionally(RetriesExhaustedException.of(attempts, request, response, throwable));
        }
    }
}
