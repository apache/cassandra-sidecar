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

package org.apache.cassandra.sidecar.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.exceptions.RangeException;
import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.stats.SSTableStats;
import org.apache.cassandra.sidecar.stats.SidecarStats;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

/**
 * General handler for serving files
 */
@Singleton
public class FileStreamer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStreamer.class);
    private static final long DEFAULT_RATE_LIMIT_STREAM_REQUESTS_PER_SECOND = Long.MAX_VALUE;

    private final ExecutorPools executorPools;
    private final ThrottleConfiguration config;
    private final SidecarRateLimiter rateLimiter;
    private final SSTableStats stats;

    @Inject
    public FileStreamer(ExecutorPools executorPools,
                        ServiceConfiguration config,
                        SidecarRateLimiter rateLimiter,
                        SidecarStats stats)
    {
        this.executorPools = executorPools;
        this.config = config.throttleConfiguration();
        this.rateLimiter = rateLimiter;
        this.stats = stats.ssTableStats();
    }

    /**
     * Streams the {@code filename file} with length {@code fileLength} for the (optionally) requested
     * {@code rangeHeader} using the provided {@code response}.
     *
     * @param response    the response to use
     * @param filename    the path to the file to serve
     * @param fileLength  the size of the file to serve
     * @param rangeHeader (optional) a string representing the requested range for the file
     * @return a future with the result of the streaming
     */
    public Future<Void> stream(HttpResponse response, String filename, long fileLength, String rangeHeader)
    {
        return parseRangeHeader(rangeHeader, fileLength)
               .compose(range -> stream(response, filename, fileLength, range));
    }

    /**
     * Streams the {@code filename file} with length {@code fileLength} for the requested
     * {@code range} using the provided {@code response}.
     *
     * @param response   the response to use
     * @param filename   the path to the file to serve
     * @param fileLength the size of the file to serve
     * @param range      the range to stream
     * @return a future with the result of the streaming
     */
    public Future<Void> stream(HttpResponse response, String filename, long fileLength, HttpRange range)
    {
        Promise<Void> promise = Promise.promise();
        acquireAndSend(response, filename, fileLength, range, Instant.now(), promise);
        return promise.future();
    }

    /**
     * Send the file if rate-limiting is disabled or when it successfully acquires a permit from the
     * {@link SidecarRateLimiter}.
     *
     * @param response   the response to use
     * @param filename   the path to the file to serve
     * @param fileLength the size of the file to serve
     * @param range      the range to stream
     * @param startTime  the start time of this request
     * @param promise    a promise for the stream
     */
    private void acquireAndSend(HttpResponse response,
                                String filename,
                                long fileLength,
                                HttpRange range,
                                Instant startTime,
                                Promise<Void> promise)
    {
        if (!isRateLimited() || acquire(response, filename, fileLength, range, startTime, promise))
        {
            // Stream data if rate limiting is disabled or if we acquire
            LOGGER.debug("Streaming range {} for file {} to client {}. Instance: {}", range, filename,
                         response.remoteAddress(), response.host());
            response.sendFile(filename, fileLength, range)
                    .onSuccess(v ->
                               {
                                   LOGGER.debug("Streamed file {} successfully to client {}. Instance: {}", filename,
                                                response.remoteAddress(), response.host());
                                   stats.onBytesStreamed(fileLength);
                                   promise.complete();
                               })
                    .onFailure(promise::fail);
        }
    }

    /**
     * Acquires a permit from the {@link SidecarRateLimiter} if it can be acquired immediately without
     * delay. Otherwise, it will retry acquiring the permit later in the future until it exhausts the
     * retry timeout, in which case it will ask the client to retry later in the future.
     *
     * @param response   the response to use
     * @param filename   the path to the file to serve
     * @param fileLength the size of the file to serve
     * @param range      the range to stream
     * @param startTime  the start time of this request
     * @param promise    a promise for the stream
     * @return {@code true} if the permit was acquired, {@code false} otherwise
     */
    private boolean acquire(HttpResponse response, String filename, long fileLength, HttpRange range, Instant startTime,
                            Promise<Void> promise)
    {
        if (rateLimiter.tryAcquire())
            return true;

        long microsToWait;
        if (checkRetriesExhausted(startTime))
        {
            LOGGER.error("Retries for acquiring permit exhausted for client {}. Instance: {}", response.remoteAddress(),
                         response.host());
            promise.fail(new HttpException(TOO_MANY_REQUESTS.code(), "Retry exhausted"));
        }
        else if ((microsToWait = rateLimiter.queryEarliestAvailable(0L))
                 < TimeUnit.SECONDS.toMicros(config.delayInSeconds()))
        {
            microsToWait = Math.max(0, microsToWait);

            LOGGER.debug("Retrying streaming after {} micros for client {}. Instance: {}", microsToWait,
                         response.remoteAddress(), response.host());
            executorPools.service()
                         .setTimer(MICROSECONDS.toMillis(microsToWait),
                                   t -> acquireAndSend(response, filename, fileLength, range, startTime, promise));
        }
        else
        {
            LOGGER.debug("Asking client {} to retry after {} micros. Instance: {}", response.remoteAddress(),
                         microsToWait, response.host());
            response.setRetryAfterHeader(microsToWait);
            promise.fail(new HttpException(TOO_MANY_REQUESTS.code(), "Ask client to retry later"));
        }
        return false;
    }

    /**
     * @return true if this request is rate-limited, false otherwise
     */
    private boolean isRateLimited()
    {
        return config.rateLimitStreamRequestsPerSecond() != DEFAULT_RATE_LIMIT_STREAM_REQUESTS_PER_SECOND;
    }

    /**
     * @param startTime the request start time
     * @return true if we exhausted the retries, false otherwise
     */
    private boolean checkRetriesExhausted(Instant startTime)
    {
        return startTime.plus(Duration.ofSeconds(config.timeoutInSeconds()))
                        .isBefore(Instant.now());
    }

    /**
     * Returns the requested range for the request, or the entire range if {@code rangeHeader} is null
     *
     * @param rangeHeader The range header from the request
     * @param fileLength  The length of the file
     * @return a succeeded future when the parsing is successful, a failed future when the range parsing fails
     */
    private Future<HttpRange> parseRangeHeader(String rangeHeader, long fileLength)
    {
        HttpRange fr = HttpRange.of(0, fileLength - 1);
        if (rangeHeader == null)
            return Future.succeededFuture(fr);

        try
        {
            // sidecar does not support multiple ranges as of now
            final HttpRange hr = HttpRange.parseHeader(rangeHeader, fileLength);
            HttpRange intersect = fr.intersect(hr);
            LOGGER.debug("Calculated range {} for streaming", intersect);
            return Future.succeededFuture(intersect);
        }
        catch (IllegalArgumentException | RangeException | UnsupportedOperationException e)
        {
            LOGGER.error(String.format("Failed to parse header '%s'", rangeHeader), e);
            return Future.failedFuture(new HttpException(REQUESTED_RANGE_NOT_SATISFIABLE.code()));
        }
    }
}
