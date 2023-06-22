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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.exception.ResourceNotFoundException;
import org.apache.cassandra.sidecar.client.request.Request;
import org.apache.cassandra.sidecar.common.http.SidecarHttpResponseStatus;

/**
 * A basic {@link RetryPolicy} supporting standard status codes
 */
public class BasicRetryPolicy extends RetryPolicy
{
    protected static final String RETRY_AFTER = "Retry-After";
    public static final int RETRY_INDEFINITELY = -1;
    protected final int maxRetries;
    protected final long retryDelayMillis;

    /**
     * Constructs a basic retry policy with unlimited number of retries and no delay between retries.
     */
    public BasicRetryPolicy()
    {
        this(RETRY_INDEFINITELY, 0);
    }

    /**
     * Constructs a basic retry policy with {@code maxRetries} number of retries and no delay between retries.
     *
     * @param maxRetries the maximum number of retries
     */
    public BasicRetryPolicy(int maxRetries)
    {
        this(maxRetries, 0);
    }

    /**
     * Constructs a basic retry policy with {@code maxRetries} number of retries and {@code retryDelayMillis} delay
     * between retries.
     *
     * @param maxRetries       the maximum number of retries
     * @param retryDelayMillis the delay between retries in milliseconds
     */
    public BasicRetryPolicy(int maxRetries, long retryDelayMillis)
    {
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
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
        // throwable can be a client connection error that prevents connecting to the remote host
        if (throwable != null ||
            response.statusCode() == HttpResponseStatus.INTERNAL_SERVER_ERROR.code())
        {
            if (canRetryOnADifferentHost)
            {
                retryImmediately(responseFuture, request, retryAction, attempts, throwable);
            }
            else
            {
                retry(responseFuture, request, retryAction, attempts, throwable);
            }
            return;
        }

        if (response.statusCode() == HttpResponseStatus.OK.code() ||
            response.statusCode() == HttpResponseStatus.PARTIAL_CONTENT.code())
        {
            responseFuture.complete(response);
            return;
        }

        if (response.statusCode() == HttpResponseStatus.NOT_FOUND.code())
        {
            if (canRetryOnADifferentHost)
            {
                retryImmediately(responseFuture, request, retryAction, attempts);
            }
            else
            {
                logger.error("Request resource not found. response={}, attempts={}", response, attempts);
                responseFuture.completeExceptionally(new ResourceNotFoundException(request));
            }
            return;
        }

        if (response.statusCode() == HttpResponseStatus.NOT_IMPLEMENTED.code())
        {
            logger.error("Request to a not implemented endpoint. response={}, attempts={}", response, attempts);
            responseFuture.completeExceptionally(unsupportedOperation(response));
            return;
        }

        if (response.statusCode() == HttpResponseStatus.SERVICE_UNAVAILABLE.code())
        {
            if (canRetryOnADifferentHost)
            {
                retryImmediately(responseFuture, request, retryAction, attempts);
            }
            else
            {
                retry(responseFuture, request, retryAction, attempts,
                      maybeParseRetryAfterOrDefault(response, attempts), null);
            }
            return;
        }

        if (response.statusCode() == HttpResponseStatus.ACCEPTED.code())
        {
            retryAction.retry(1, retryDelayMillis(1));
            return;
        }

        if (response.statusCode() == SidecarHttpResponseStatus.CHECKSUM_MISMATCH.code())
        {
            // assume that the uploaded payload might have been corrupted, so allow for retries when an invalid
            // checksum is encountered
            if (canRetryOnADifferentHost)
            {
                retryImmediately(responseFuture, request, retryAction, attempts, throwable);
            }
            else
            {
                retry(responseFuture, request, retryAction, attempts, throwable);
            }
            return;
        }

        // 4xx Client Errors - 5xx Server Errors
        if (HttpStatusClass.CLIENT_ERROR.contains(response.statusCode()) ||
            HttpStatusClass.SERVER_ERROR.contains(response.statusCode()))
        {
            if (canRetryOnADifferentHost)
            {
                retryImmediately(responseFuture, request, retryAction, attempts);
            }
            else
            {
                logger.error("Request exhausted. response={}, attempts={}", response, attempts);
                responseFuture.completeExceptionally(retriesExhausted(attempts, request));
            }
            return;
        }

        logger.error("Request encountered an unexpected status code. response={}, attempts={}", response, attempts);
        responseFuture.completeExceptionally(unexpectedStatusCode(response));
    }

    /**
     * @return the maximum number of retries configured for this retry policy
     */
    protected int maxRetries()
    {
        return maxRetries;
    }

    /**
     * Returns the number of milliseconds to wait before attempting the next request.
     *
     * @param attempts the number of attempts already performed for this request
     * @return the number of milliseconds to wait before attempting the next request
     */
    protected long retryDelayMillis(int attempts)
    {
        return retryDelayMillis;
    }

    /**
     * Retries the request with no delay
     *
     * @param future      a future for the {@link HttpResponse}
     * @param request     the HTTP request
     * @param retryAction the action that is called on retry
     * @param attempts    the number of attempts for the request
     */
    protected void retryImmediately(CompletableFuture<HttpResponse> future,
                                    Request request,
                                    RetryAction retryAction,
                                    int attempts)
    {
        retry(future, request, retryAction, attempts, 0L, null);
    }

    /**
     * Retries the request with no delay
     *
     * @param future      a future for the {@link HttpResponse}
     * @param request     the HTTP request
     * @param retryAction the action that is called on retry
     * @param attempts    the number of attempts for the request
     * @param throwable   the underlying exception
     */
    protected void retryImmediately(CompletableFuture<HttpResponse> future,
                                    Request request,
                                    RetryAction retryAction,
                                    int attempts,
                                    Throwable throwable)
    {
        retry(future, request, retryAction, attempts, 0L, throwable);
    }

    /**
     * Retries the request after waiting for the configured retryDelayMillis
     *
     * @param future      a future for the {@link HttpResponse}
     * @param request     the HTTP request
     * @param retryAction the action that is called on retry
     * @param attempts    the number of attempts for the request
     * @param throwable   the underlying exception
     */
    protected void retry(CompletableFuture<HttpResponse> future,
                         Request request,
                         RetryAction retryAction,
                         int attempts,
                         Throwable throwable)
    {
        retry(future, request, retryAction, attempts, retryDelayMillis(attempts), throwable);
    }

    /**
     * Retries the request after waiting for {@code sleepTimeMillis}. If the retries have exceeded the maximum number
     * of retries allowed, it completes exceptionally with a
     * {@link org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException}.
     *
     * @param future          a future for the {@link HttpResponse}
     * @param request         the HTTP request
     * @param retryAction     the action that is called on retry
     * @param attempts        the number of attempts for the request
     * @param sleepTimeMillis the amount of time to wait in milliseconds before attempting the request again
     * @param throwable       the underlying error
     */
    protected void retry(CompletableFuture<HttpResponse> future,
                         Request request,
                         RetryAction retryAction,
                         int attempts,
                         long sleepTimeMillis,
                         Throwable throwable)
    {
        int configuredMaxRetries = maxRetries();
        if (configuredMaxRetries > RETRY_INDEFINITELY && attempts >= configuredMaxRetries)
        {
            future.completeExceptionally(retriesExhausted(attempts, request, throwable));
        }
        else
        {
            retryAction.retry(attempts + 1, sleepTimeMillis);
        }
    }

    /**
     * Tries to parse the {@code Retry-After} header from the response, and if successful, returns the number of
     * milliseconds to wait specified by the header. If it fails to parse, returns the result of
     * {@link #retryDelayMillis(int)}.
     *
     * @param response the HTTP response
     * @param attempts the number of attempts for the request
     * @return the delay to wait specified by the request if available, or the default specified by
     * {@link #retryDelayMillis(int)}
     */
    protected long maybeParseRetryAfterOrDefault(HttpResponse response, int attempts)
    {
        List<String> retryAfter = response.headers().get(RETRY_AFTER);
        if (retryAfter != null && !retryAfter.isEmpty())
        {
            try
            {
                // <delay-seconds> spec is in seconds - convert to millis
                long seconds = Long.parseLong(retryAfter.get(0));
                return TimeUnit.SECONDS.toMillis(seconds);
            }
            catch (NumberFormatException e)
            {
                logger.warn("Failed to parse header={}, value={}", RETRY_AFTER, retryAfter.get(0), e);
            }
        }
        return retryDelayMillis(attempts);
    }
}
