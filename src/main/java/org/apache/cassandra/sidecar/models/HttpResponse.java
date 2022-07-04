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

package org.apache.cassandra.sidecar.models;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.sidecar.utils.RequestUtils.extractHostAddressWithoutPort;

/**
 * Wrapper around HttpServerResponse
 */
public class HttpResponse
{
    private final String host;
    private final HttpServerRequest request;
    private final HttpServerResponse response;

    public HttpResponse(HttpServerRequest request, HttpServerResponse response)
    {
        this.request = request;
        this.response = response;
        this.host = extractHostAddressWithoutPort(request.host());
    }

    public void setRetryAfterHeader(long microsToWait)
    {
        response.setStatusCode(HttpResponseStatus.TOO_MANY_REQUESTS.code());
        response.putHeader(HttpHeaderNames.RETRY_AFTER, Long.toString(MICROSECONDS.toSeconds(microsToWait))).end();
    }

    public void setTooManyRequestsStatus()
    {
        response.setStatusCode(HttpResponseStatus.TOO_MANY_REQUESTS.code()).end();
    }

    public void setRangeNotSatisfiable(String msg)
    {
        response.setStatusCode(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE.code()).setStatusMessage(msg).end();
    }

    public void setPartialContentStatus(Range range)
    {
        response.setStatusCode(HttpResponseStatus.PARTIAL_CONTENT.code())
                .putHeader(HttpHeaderNames.CONTENT_RANGE, contentRangeHeader(range));
    }

    private String contentRangeHeader(Range r)
    {
        return "bytes " + r.start() + "-" + r.end() + "/" + r.length();
    }

    public void setBadRequestStatus(String msg)
    {
        response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).setStatusMessage(msg).end();
    }

    public void setNotFoundStatus(String msg)
    {
        response.setStatusCode(HttpResponseStatus.NOT_FOUND.code()).setStatusMessage(msg).end();
    }

    public void setInternalErrorStatus(String msg)
    {
        response.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).setStatusMessage(msg).end();
    }

    /**
     * Send a range in a file asynchronously
     *
     * @param fileName   file to send
     * @param fileLength the size of the file to send
     * @param range      range to send
     * @return a future completed with the body result
     */
    public Future<Void> sendFile(String fileName, long fileLength, Range range)
    {
        // notify client we support range requests
        response.putHeader(HttpHeaders.ACCEPT_RANGES, "bytes");

        if (range.length() != fileLength)
        {
            setPartialContentStatus(range);
        }

        return response.putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_OCTET_STREAM)
                       .putHeader(HttpHeaderNames.CONTENT_LENGTH, Long.toString(range.length()))
                       .sendFile(fileName, range.start(), range.length());
    }

    /**
     * @return the remote address for this connection, possibly {@code null} (e.g a server bound on a domain socket).
     */
    public SocketAddress remoteAddress()
    {
        return request.remoteAddress();
    }

    /**
     * @return the request host without the port
     */
    public String host()
    {
        return host;
    }
}
