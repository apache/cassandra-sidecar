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

package org.apache.cassandra.sidecar.client;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.cassandra.sidecar.client.request.DecodableRequest;
import org.apache.cassandra.sidecar.client.request.Request;

import static java.util.Objects.requireNonNull;

/**
 * Executes requests to Cassandra Sidecar
 */
public class RequestExecutor implements AutoCloseable
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final HttpClient httpClient;
    protected final ScheduledExecutorService singleThreadExecutorService;

    protected RequestExecutor(HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "The httpClient is required");
        this.singleThreadExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Executes the request and waits if necessary for at most the configured time in the
     * {@link HttpClientConfig#timeoutMillis()} for this future to complete, and then returns its result, if available.
     *
     * @param context the request context
     * @param <T>     the expected type for the instance
     * @return the result value
     * @throws CancellationException if this future was cancelled
     * @throws ExecutionException    if this future completed exceptionally
     * @throws InterruptedException  if the current thread was interrupted while waiting
     * @throws TimeoutException      if the wait timed out
     */
    public <T> T executeRequest(RequestContext context)
    throws ExecutionException, InterruptedException, TimeoutException
    {
        return executeRequest(context, httpClient.config().timeoutMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the request and waits if necessary for at most the provided {@code timeout} with units {@code unit}
     * for this future to complete, and then returns its result, if available.
     *
     * @param context the request context
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @param <T>     the expected type for the instance
     * @return the result value
     * @throws CancellationException if this future was cancelled
     * @throws ExecutionException    if this future completed exceptionally
     * @throws InterruptedException  if the current thread was interrupted while waiting
     * @throws TimeoutException      if the wait timed out
     */
    public <T> T executeRequest(RequestContext context, long timeout, TimeUnit unit)
    throws ExecutionException, InterruptedException, TimeoutException
    {
        return this.<T>executeRequestAsync(context).get(timeout, unit);
    }

    /**
     * Returns the expected instance of type {@code <T>} after executing the {@code request} and processing it.
     *
     * @param context the request context
     * @param <T>     the expected type for the instance
     * @return the expected instance of type {@code <T>} after executing the {@code request} and processing it
     */
    public <T> CompletableFuture<T> executeRequestAsync(RequestContext context)
    {
        Iterator<SidecarInstance> iterator = context.instanceSelectionPolicy().iterator();
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        if (!iterator.hasNext())
        {
            resultFuture.completeExceptionally(new IllegalStateException("InstanceSelectionPolicy " +
                                                                         context.instanceSelectionPolicy()
                                                                                .getClass()
                                                                                .getSimpleName() +
                                                                         " selects 0 instances"));
            return resultFuture;
        }
        SidecarInstance instance = iterator.next();
        CompletableFuture<HttpResponse> responseFuture = new CompletableFuture<>();
        executeWithRetries(responseFuture, iterator, instance, context, 1);

        responseFuture.whenComplete((response, retryThrowable) ->
                                    processResponse(resultFuture, context.request(), response, retryThrowable));

        return resultFuture;
    }

    /**
     * Streams the request from the context to the {@code streamConsumer}.
     *
     * @param context        the request context
     * @param streamConsumer the object that consumes the stream
     */
    public void streamRequest(RequestContext context, StreamConsumer streamConsumer)
    {
        Objects.requireNonNull(streamConsumer, "streamConsumer must be non-null");
        Iterator<SidecarInstance> iterator = context.instanceSelectionPolicy().iterator();
        if (!iterator.hasNext())
        {
            streamConsumer.onError(new IllegalStateException("InstanceSelectionPolicy " +
                                                             context.instanceSelectionPolicy()
                                                                    .getClass()
                                                                    .getSimpleName() +
                                                             " selects 0 instances"));
            return;
        }
        SidecarInstance instance = iterator.next();
        CompletableFuture<HttpResponse> responseFuture = new CompletableFuture<>();
        streamWithRetries(responseFuture, streamConsumer, iterator, instance, context, 1);

        responseFuture.whenComplete(((response, throwable) -> {
            if (throwable != null)
            {
                streamConsumer.onError(throwable);
            }
        }));
    }

    /**
     * Closes the underlying HTTP client
     */
    public void close() throws Exception
    {
        httpClient.close();
    }

    /**
     * Executes the {@code request} from the {@code context} on the provided {@code sidecarInstance}, and applies the
     * retry policy after complete.
     *
     * @param future          a future for the {@link HttpResponse}
     * @param iterator        the iterator of instances
     * @param sidecarInstance the Sidecar instance where the request will be performed
     * @param context         the request context
     * @param attempt         the number of attempts for this request
     */
    protected void executeWithRetries(CompletableFuture<HttpResponse> future,
                                      Iterator<SidecarInstance> iterator,
                                      SidecarInstance sidecarInstance,
                                      RequestContext context,
                                      int attempt)
    {
        logger.debug("Request from instance={}, request={}, attempt={}",
                     sidecarInstance, context.request(), attempt);

        // execute the http request and process the response with the retry policy
        try
        {
            httpClient.execute(sidecarInstance, context)
                      .whenComplete((HttpResponse response, Throwable throwable) ->
                                    applyRetryPolicy(future,
                                                     iterator,
                                                     sidecarInstance,
                                                     context,
                                                     attempt,
                                                     response,
                                                     throwable));
        }
        catch (Throwable throwable)
        {
            logger.error("Unexpected error while executing the request. instance={}, request={}, attempt={}",
                         sidecarInstance, context.request(), attempt);
            future.completeExceptionally(throwable);
        }
    }

    /**
     * Streams the request from the {@code context} to the {@code streamConsumer} from the provided
     * {@code sidecarInstance}, and applies the retry policy after inspecting the response headers.
     *
     * @param future          a future for the {@link HttpResponse}
     * @param streamConsumer  the object that consumes the stream
     * @param iterator        the iterator of instances
     * @param sidecarInstance the Sidecar instance where the request will be performed
     * @param context         the request context
     * @param attempt         the number of attempts for this request
     */
    private void streamWithRetries(CompletableFuture<HttpResponse> future,
                                   StreamConsumer streamConsumer,
                                   Iterator<SidecarInstance> iterator,
                                   SidecarInstance sidecarInstance,
                                   RequestContext context,
                                   int attempt)
    {
        logger.debug("Streaming from instance={}, request={}, attempt={}",
                     sidecarInstance, context.request(), attempt);

        try
        {
            httpClient.stream(sidecarInstance, context, streamConsumer)
                      .whenComplete((HttpResponse response, Throwable throwable) ->
                                    applyRetryPolicy(future,
                                                     streamConsumer,
                                                     iterator,
                                                     sidecarInstance,
                                                     context,
                                                     attempt,
                                                     response,
                                                     throwable));
        }
        catch (Throwable throwable)
        {
            logger.error("Unexpected error while streaming. instance={}, request={}, attempt={}",
                         sidecarInstance, context.request(), attempt);
            future.completeExceptionally(throwable);
        }
    }

    /**
     * Applies the {@code retryPolicy} to the response. The request will be retried based on the policy.
     *
     * @param future          the future for the {@link HttpResponse}
     * @param iterator        the iterator of instances
     * @param sidecarInstance the Sidecar instance where the request was performed
     * @param context         the request context
     * @param attempt         the number of attempts for this request
     * @param response        the {@link HttpResponse} received from the server
     * @param throwable       the error encountered during the request, or null if no error was encountered
     */
    private void applyRetryPolicy(CompletableFuture<HttpResponse> future,
                                  Iterator<SidecarInstance> iterator,
                                  SidecarInstance sidecarInstance,
                                  RequestContext context,
                                  final int attempt,
                                  HttpResponse response,
                                  Throwable throwable)
    {
        boolean retryOnNewHost = iterator.hasNext();
        // check status code and apply retry policy on invalid status code
        Request request = context.request();
        context.retryPolicy()
               .onResponse(future, request, response, throwable, attempt, retryOnNewHost, (nextAttempt, delay) -> {
            String statusCode = response != null ? String.valueOf(response.statusCode()) : "<Not Available>";
            SidecarInstance nextInstance = iterator.hasNext() ? iterator.next() : sidecarInstance;
            if (response == null || response.statusCode() != HttpResponseStatus.ACCEPTED.code())
            {
                logger.warn("Retrying request on {} instance after {}ms. " +
                            "Failed on instance={}, attempt={}, statusCode={}",
                            nextInstance == sidecarInstance ? "same" : "next", delay,
                            sidecarInstance, attempt, statusCode, throwable);
            }
            schedule(delay, () -> executeWithRetries(future, iterator, nextInstance, context, nextAttempt));
        });
    }

    /**
     * Applies the {@code retryPolicy} to the stream. The stream will be retried based on the policy.
     *
     * @param future          the future for the {@link HttpResponse}
     * @param consumer        the object that consumes the stream
     * @param iterator        the iterator of instances
     * @param sidecarInstance the Sidecar instance where the request was performed
     * @param context         the request context
     * @param attempt         the number of attempts for this request
     * @param response        the {@link HttpResponse} received from the server
     * @param throwable       the error encountered during the request, or null if no error was encountered
     */
    private void applyRetryPolicy(CompletableFuture<HttpResponse> future,
                                  StreamConsumer consumer,
                                  Iterator<SidecarInstance> iterator,
                                  SidecarInstance sidecarInstance,
                                  RequestContext context,
                                  final int attempt,
                                  HttpResponse response,
                                  Throwable throwable)
    {
        boolean retryOnNewHost = iterator.hasNext();
        // check status code and apply retry policy on invalid status code
        Request request = context.request();
        context.retryPolicy()
               .onResponse(future, request, response, throwable, attempt, retryOnNewHost, (nextAttempt, delay) -> {
            String statusCode = response != null ? String.valueOf(response.statusCode()) : "<Not Available>";
            SidecarInstance nextInstance = iterator.hasNext() ? iterator.next() : sidecarInstance;
            if (response == null || response.statusCode() != HttpResponseStatus.ACCEPTED.code())
            {
                logger.warn("Retrying stream on {} instance after {}ms. " +
                            "Failed on instance={}, attempt={}, statusCode={}",
                            nextInstance == sidecarInstance ? "same" : "next", delay,
                            sidecarInstance, attempt, statusCode, throwable);
            }
            schedule(delay, () -> streamWithRetries(future, consumer, iterator, nextInstance, context, nextAttempt));
        });
    }

    /**
     * Processes the {@code response} result and sets the future as a completed future or as a completed exceptionally
     * future when an error occurred during processing.
     *
     * @param future    the future for the request
     * @param request   the request
     * @param response  the {@link HttpResponse} received from the server
     * @param throwable the error encountered during the request, or null if no error was encountered
     * @param <T>       the type expected by the requester
     */
    @SuppressWarnings("unchecked")
    private <T> void processResponse(CompletableFuture<T> future,
                                     Request request,
                                     HttpResponse response,
                                     Throwable throwable)
    {
        if (throwable != null)
        {
            logger.error("Failed to process request={}, response={}", request, response, throwable);
            future.completeExceptionally(throwable);
            return;
        }

        try
        {
            if (request instanceof DecodableRequest)
            {
                DecodableRequest<T> decodableRequest = (DecodableRequest<T>) request;
                future.complete(decodableRequest.decode(response.raw()));
            }
            else
            {
                future.complete((T) response.contentAsString());
            }
        }
        catch (Throwable t)
        {
            future.completeExceptionally(t);
        }
    }

    /**
     * Schedule the {@code runnable} after {@code delayMillis} milliseconds.
     *
     * @param delayMillis the delay before retrying in milliseconds
     * @param runnable    the code to execute
     */
    protected void schedule(long delayMillis, Runnable runnable)
    {
        if (delayMillis > 0)
        {
            singleThreadExecutorService.schedule(runnable, delayMillis, TimeUnit.MILLISECONDS);
        }
        runnable.run();
    }
}
